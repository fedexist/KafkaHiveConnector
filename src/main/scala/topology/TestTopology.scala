package topology

import java.util

import org.apache.storm.hive.bolt.HiveBolt
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper
import org.apache.storm.hive.common.HiveOptions
import org.apache.storm.hive.trident.{HiveStateFactory, HiveUpdater}
import org.apache.storm.kafka.{KafkaSpout, SpoutConfig, ZkHosts}
import org.apache.storm.{Config, StormSubmitter, trident}
import org.apache.storm.trident.{TridentState, TridentTopology}
import org.apache.storm.trident.state.StateFactory
import org.apache.storm.tuple.Fields

import scala.collection.JavaConverters._



class TestTopology {

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
    val kafkaConfig = new SpoutConfig(zkHosts, "air_traffic", "", "storm")
    val kafkaSpout = new KafkaSpout(kafkaConfig)

    //Topology
    val topology: TridentTopology = new TridentTopology
    val stream: trident.Stream = topology.newStream("eventsEmitter", kafkaSpout)


    val factory: StateFactory = new HiveStateFactory().withOptions(hiveOptions)
    val state: TridentState = stream.partitionPersist(factory, new Fields(colNames), new HiveUpdater(), new Fields())
    //builder.setBolt("hiveProcessor", hiveBolt, 8).fieldsGrouping("requestsEmitter", new Fields(colNames))

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
