package topology

import java.util

import org.apache.storm.hive.bolt.HiveBolt
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper
import org.apache.storm.hive.common.HiveOptions
import org.apache.storm.hive.trident.{HiveStateFactory, HiveUpdater}
import org.apache.storm.kafka.{BrokerHosts, KafkaSpout, SpoutConfig, ZkHosts}
import org.apache.storm.{Config, StormSubmitter, trident}
import org.apache.storm.trident.{TridentState, TridentTopology}
import org.apache.storm.trident.state.StateFactory
import org.apache.storm.tuple.{Fields, Values}
import org.apache.storm.kafka.trident.{OpaqueTridentKafkaSpout, TridentKafkaConfig}
import org.apache.storm.trident.operation.BaseFunction
import org.apache.storm.trident.operation.TridentCollector
import org.apache.storm.trident.tuple.TridentTuple

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSON



class TestTopology {



  class ParseJSON extends BaseFunction {

    private val HTTP_CODE_200 = 200
    /**
      * Takes a tuple adds the RDNS and emits a new tuple.
      *
      * @param tuple     an TridentTuple that contains fields in JSON format
      * @param collector the TridentCollector
      **/
    override final def execute(tuple: TridentTuple, collector: TridentCollector): Unit = {
      val bytes = tuple.getBinary(0)
      try {
        val decoded = new String(bytes)
        val result = JSON.parseFull(decoded)
        result match {
          case Some(json: Map[String, Any]) => collector.emit(new Values( json("origin"),
                                                                          json("flight"),
                                                                          json("course"),
                                                                          json("aircraft"),
                                                                          json("callsign"),
                                                                          json("registration"),
                                                                          json("lat"),
                                                                          json("speed"),
                                                                          json("altitude"),
                                                                          json("destination"),
                                                                          json("lon"),
                                                                          json("time")))
          case None => println("Parsing failed")
          case other => println("Unknown data structure: " + other)
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val zkIp = "master-1.localdomain"
    val nimbusHost = "master-2.localdomain"
    val metaStoreURI = "master-1.localdomain"
    val dbName = "data_stream"
    val tblName = "air_traffic"
    val zookeeperHost: String = zkIp + ":2181"
    val zkHosts = new ZkHosts(zookeeperHost)
    val colNames: util.List[String] = Seq("origin", "flight", "course", "aircraft", "callsign",
      "registration", "lat", "lon", "altitude", "speed", "destination", "time").asJava

    //HiveBolt
    val mapper: DelimitedRecordHiveMapper =
      new DelimitedRecordHiveMapper()
        .withColumnFields(new Fields(colNames))
        .withTimeAsPartitionField("YYYY/MM/DD")
    val hiveOptions: HiveOptions =
      new HiveOptions(metaStoreURI, dbName, tblName, mapper)
        .withTxnsPerBatch(10)
        .withBatchSize(1000)
        .withIdleTimeout(10)
    val hiveBolt = new HiveBolt(hiveOptions)

    //KafkaSpout
    val broker = new ZkHosts("localhost")
    val spoutConf = new TridentKafkaConfig(broker, "air_traffic")
    val kafkaSpout = new OpaqueTridentKafkaSpout(spoutConf)

    //Topology
    val topology: TridentTopology = new TridentTopology
    val factory: StateFactory = new HiveStateFactory().withOptions(hiveOptions)
    val stream: trident.Stream = topology.newStream("jsonEmitter", kafkaSpout)
                                  .each(new Fields("bytes"), new ParseJSON , new Fields(colNames))

    stream.partitionPersist(factory, new Fields(colNames), new HiveUpdater(), new Fields()).parallelismHint(8)

    //Storm Config
    val config = new Config
    config.setMaxTaskParallelism(5)
    config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, new Integer(2))
    config.put(Config.NIMBUS_SEEDS, nimbusHost)
    config.put(Config.NIMBUS_THRIFT_PORT, new Integer(6627))
    config.put(Config.STORM_ZOOKEEPER_PORT, new Integer(2181))
    config.put(Config.STORM_ZOOKEEPER_SERVERS, util.Arrays.asList(zkIp))

    try
      StormSubmitter.submitTopology("air-traffic-topology", config, topology.build())
    catch {
      case e: Exception =>
        throw new IllegalStateException("Couldn't initialize the topology", e)
    }

  }

}
