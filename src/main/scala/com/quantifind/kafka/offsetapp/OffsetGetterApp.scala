package com.quantifind.kafka.offsetapp

import java.text.NumberFormat

import com.quantifind.kafka.OffsetGetter
import com.quantifind.sumac.validation.Required
import com.quantifind.sumac.{ArgMain, FieldArgs}
import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

import scala.concurrent.duration._

class OffsetGetterArgsWGT extends OffsetGetterArgs {
  @Required
  var group: String = _

  var topics: Seq[String] = Seq()

  var sumPart: Boolean = false

  var onlyOffsets: Boolean = false

  // if you want your lag results formatted with thousands separator
  var formatLagOutput = false
}

class OffsetGetterArgs extends FieldArgs {
  @Required
  var zk: String = _

  var brokerList: Option[String] = None
  var brokerStorage: Boolean = false

  var zkSessionTimeout: Duration = 30 seconds
  var zkConnectionTimeout: Duration = 30 seconds

  addValidation {
    require(!brokerStorage || (brokerList.isDefined && brokerList.get.nonEmpty),
      "brokerList is required when using brokerStorage")
  }
}

/**
 * TODO DOC
 * User: pierre
 * Date: 1/22/14
 */
object OffsetGetterApp extends ArgMain[OffsetGetterArgsWGT] {

  def main(args: OffsetGetterArgsWGT) {
    var zkClient: ZkClient = null
    var og: OffsetGetter = null
    var brokerList: Option[Seq[Broker]] = null
    try {
      zkClient = new ZkClient(args.zk,
        args.zkSessionTimeout.toMillis.toInt,
        args.zkConnectionTimeout.toMillis.toInt,
        ZKStringSerializer)
      brokerList = args.brokerList.map(ClientUtils.parseBrokerList)

      og = new OffsetGetter(zkClient, args.brokerStorage, brokerList)
      val i = og.getInfo(args.group, args.topics)

      if (i.offsets.nonEmpty) {
        if (!args.onlyOffsets) {
          println()
        }
        if (args.sumPart) {
          if (!args.onlyOffsets) {
            println("%-15s\t%-40s\t%-15s\t%-15s\t%-15s".format("Group", "Topic", "Offset", "logSize", "Lag"))
          }
          i.offsets.groupBy(info => (info.group, info.topic))
            .flatMap {
              case (_, infoGrp) =>
                infoGrp.headOption map { head =>
                  val (offset, log, lag) = infoGrp.foldLeft((0l, 0l, 0l)) {
                    case ((offAcc, logAcc, lagAcc), info) =>
                      (offAcc + info.offset, logAcc + info.logSize, lagAcc + info.lag)
                  }
                  val fmtedLag = if (args.formatLagOutput) NumberFormat.getIntegerInstance().format(lag) else lag
                  "%-15s\t%-40s\t%-15s\t%-15s\t%-15s".format(head.group, head.topic, offset, log, fmtedLag)
                }
            }.foreach(println)
        } else {
          if (!args.onlyOffsets) {
            println("%-15s\t%-40s\t%-3s\t%-15s\t%-15s\t%-15s\t%s".format("Group", "Topic", "Pid", "Offset", "logSize", "Lag", "Owner"))
          }
          i.offsets.foreach {
            info =>
              val fmtedLag = if (args.formatLagOutput) NumberFormat.getIntegerInstance().format(info.lag) else info.lag
              println("%-15s\t%-40s\t%-3s\t%-15s\t%-15s\t%-15s\t%s".format(info.group, info.topic, info.partition, info.offset, info.logSize, fmtedLag,
                info.owner.getOrElse("none")))
          }
        }

        if (!args.onlyOffsets) {
          println()
          println("Brokers")
          i.brokers.foreach {
            b =>
              println(s"${b.id}\t${b.host}:${b.port}")
          }
        }
      } else {
        System.err.println(s"no topics for group ${args.group}")
      }

    } finally {
      if (og != null) og.close()
      if (zkClient != null)
        zkClient.close()
    }
  }

}

