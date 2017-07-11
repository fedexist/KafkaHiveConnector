package topology

import java.util

import org.apache.storm.hive.bolt.mapper.{DelimitedRecordHiveMapper, JsonRecordHiveMapper}
import org.apache.storm.hive.common.HiveOptions
import org.apache.storm.hive.trident.{HiveStateFactory, HiveUpdater}
import org.apache.storm.kafka._
import org.apache.storm.{Config, StormSubmitter, trident}
import org.apache.storm.trident.{TridentState, TridentTopology}
import org.apache.storm.trident.state.StateFactory
import org.apache.storm.tuple.{Fields, Values}
import org.apache.storm.kafka.trident.{OpaqueTridentKafkaSpout, TridentKafkaConfig}
import org.apache.storm.spout.SchemeAsMultiScheme
import org.apache.storm.trident.operation.BaseFunction
import org.apache.storm.trident.operation.TridentCollector
import org.apache.storm.trident.tuple.TridentTuple

import scala.collection.JavaConverters._
import spray.json._
import DefaultJsonProtocol._

object TestTopology extends App {



  class ParseJSON extends BaseFunction {
    /**
      * Takes a tuple adds the RDNS and emits a new tuple.
      *
      * @param tuple     an TridentTuple that contains fields in JSON format
      * @param collector the TridentCollector
      **/
    override final def execute(tuple: TridentTuple, collector: TridentCollector): Unit = {
      val json_string = tuple.getString(0)
      var result: JsValue = null
      try {
        result = json_string.parseJson
      }
      catch {
        case unknown :Exception => println("could not parse" + json_string)
      }
      finally {
      result match {
        case json: JsObject  => collector.emit(new Values(
                                    json.fields("origin").toString,
                                    json.fields("flight").toString,
          new Integer(json.fields("course").toString.toInt),
                                    json.fields("aircraft").toString,
                                    json.fields("callsign").toString,
                                    json.fields("registration").toString,
          new java.lang.Double(json.fields("lat").toString.toDouble),
          new Integer(json.fields("speed").toString.toInt),
          new Integer(json.fields("altitude").toString.toInt),
                                    json.fields("destination").toString,
          new java.lang.Double(json.fields("lon").toString.toDouble),
          new java.lang.Long(json.fields("time").toString.toLong)))
        case other => println("Unknown data structure: " + other)
        }
      }
    }
  }

  override def main(args: Array[String]): Unit = {

    val master_1 = "master-1.localdomain"
    val master_2 = "master-2.localdomain"
    val metastore = "thrift://master-1.localdomain:9083,thrift://master-2.localdomain:9083"    
    val dbName = "data_stream"
    val tblName = "air_traffic_test2"
    val zkHosts_1 = new ZkHosts(master_1 + ":2181")
    val zkHosts_2 = new ZkHosts(master_2 + ":2181")
    val colNames: util.List[String] = Seq("origin", "flight", "course", "aircraft", "callsign",
      "registration", "lat", "lon", "altitude", "speed", "destination", "time").asJava

    //HiveBolt
    val mapper: JsonRecordHiveMapper  =
      new JsonRecordHiveMapper()
        .withColumnFields(new Fields(colNames))
        //.withTimeAsPartitionField("YYYY/MM/DD")
    val hiveOptions: HiveOptions =
      new HiveOptions(metastore, dbName, tblName, mapper)
        .withTxnsPerBatch(10)
        .withBatchSize(1000)
        .withIdleTimeout(10)

    //KafkaSpout
    val spoutConf = new TridentKafkaConfig(zkHosts_2, "air_traffic")
    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme())
    val kafkaSpout = new OpaqueTridentKafkaSpout(spoutConf)

    //Topology
    val topology: TridentTopology = new TridentTopology
    val factory: StateFactory = new HiveStateFactory().withOptions(hiveOptions)
    val stream: trident.Stream = topology.newStream("jsonEmitter", kafkaSpout)
                                  .each(new Fields("str"), new ParseJSON , new Fields(colNames))

    stream.partitionPersist(factory, new Fields(colNames), new HiveUpdater(), new Fields()).parallelismHint(8)

    //Storm Config
    val config = new Config
    config.setMaxTaskParallelism(5)
    config.put(Config.NIMBUS_SEEDS, util.Arrays.asList(master_2))
    config.put(Config.NIMBUS_THRIFT_PORT, new Integer(6627))
    config.put(Config.STORM_ZOOKEEPER_PORT, new Integer(2181))
    config.put(Config.STORM_ZOOKEEPER_SERVERS, util.Arrays.asList(master_1, master_2))

    try
      StormSubmitter.submitTopology("air-traffic-topology", config, topology.build)
    catch {
      case e: Exception =>
        throw new IllegalStateException("Couldn't initialize the topology", e)
    }

  }

}
