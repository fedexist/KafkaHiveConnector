package topology

import java.util

import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper
import org.apache.storm.hive.common.HiveOptions
import org.apache.storm.hive.trident.{HiveStateFactory, HiveUpdater}
import org.apache.storm.kafka._
import org.apache.storm.{Config, StormSubmitter, trident}
import org.apache.storm.trident.{TridentState, TridentTopology}
import org.apache.storm.trident.state.StateFactory
import org.apache.storm.tuple.{Fields, Values}
import org.apache.storm.kafka.trident.{TransactionalTridentKafkaSpout, TridentKafkaConfig}
import org.apache.storm.spout.SchemeAsMultiScheme
import org.apache.storm.trident.operation.BaseFunction
import org.apache.storm.trident.operation.TridentCollector
import org.apache.storm.trident.tuple.TridentTuple
import org.joda.time.{DateTime, DateTimeZone}
import play.api.libs.json.{JsValue, Json}

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

object TestTopology extends App {

  object idLookup extends BaseFunction{

    var idLookupMap = new TrieMap[String,(Int, Long)]
    var lastId = 0

    def isActive(tuple: (String, (Int, Long))) : Boolean = DateTime.now(DateTimeZone.UTC).getMillis - tuple._2._2 < 60000

    def createOrGetId(_key : String, _time : Long) : Int = {

        idLookupMap = idLookupMap filterNot isActive

        val entry = idLookupMap.get(_key)

        entry match {

            case Some((id, time))  => idLookupMap+=((_key, (id, _time))); id
            case None => idLookupMap+=((_key, (lastId, _time))); lastId+=1; lastId - 1

        }

    }

    /**
      * Takes a tuple adds the RDNS and emits a new tuple.
      *
      * @param tuple     an TridentTuple that contains fields in JSON format
      * @param collector the TridentCollector
      **/
    override final def execute(tuple: TridentTuple, collector: TridentCollector): Unit = {

      val key = tuple.getString(1)
      val time = tuple.getLong(11)

      collector.emit(new Values(new Integer(createOrGetId(key, time))))

    }

  }



  class ParseJSON extends BaseFunction {
    /**
      * Takes a tuple adds the RDNS and emits a new tuple.
      *
      * @param tuple     an TridentTuple that contains fields in JSON format
      * @param collector the TridentCollector
      **/
    override final def execute(tuple: TridentTuple, collector: TridentCollector): Unit = {

      val json_string: String = tuple.getString(0)
      val result: JsValue = Json.parse(json_string)


      try{
      collector.emit(new Values(
          result("origin").toString,
          result("flight").toString,
          new Integer(result("course").toString.toInt),
          result("aircraft").toString,
          result("callsign").toString,
          result("registration").toString,
          new java.lang.Double(result("lat").toString.toDouble),
          new Integer(result("speed").toString.toInt),
          new Integer(result("altitude").toString.toInt),
          result("destination").toString,
          new java.lang.Double(result("lon").toString.toDouble),
          new java.lang.Long(result("time").toString.toLong)))
      } catch {

        case e: Exception => println("Error parsing: " + json_string); println(e)

      }
    }
  }

  override def main(args: Array[String]): Unit = {

    val master_1 = "master-1.localdomain"
    val master_2 = "master-2.localdomain"
    val metastore = "thrift://master-1.localdomain:9083"
    val dbName = "data_stream"
    val tblName = Seq("daily_table", "real_time_table", "active_table")
    val zkHosts_1 = new ZkHosts(master_1 + ":2181")
    val zkHosts_2 = new ZkHosts(master_2 + ":2181")
    val json_fields = Seq("origin", "flight", "course", "aircraft", "callsign",
      "registration", "lat", "speed", "altitude", "destination", "lon", "time")

    val daily_columns = Seq("id", "flight", "aircraft", "callsign", "registration", "origin", "destination")
    val realtime_columns = Seq("id", "lat", "lon", "altitude", "speed", "time", "course")
    val active_columns = Seq("id", "origin", "destination", "lat", "lon", "time", "aircraft")

    //HiveBolt
    val mapper_daily: DelimitedRecordHiveMapper  =
      new DelimitedRecordHiveMapper()
        .withColumnFields(new Fields(daily_columns.asJava))
        .withTimeAsPartitionField("YYYY-MM-DD'T'HH")
    val hiveOptions_daily: HiveOptions =
      new HiveOptions(metastore, dbName, tblName.head, mapper_daily)
        .withTxnsPerBatch(10)
        .withBatchSize(1000)
        .withIdleTimeout(10)
    val factory_daily: StateFactory = new HiveStateFactory().withOptions(hiveOptions_daily)


    val mapper_realtime: DelimitedRecordHiveMapper  =
      new DelimitedRecordHiveMapper()
        .withColumnFields(new Fields(realtime_columns.asJava))
        .withTimeAsPartitionField("YYYY-MM-DD'T'HH")
    val hiveOptions_realtime: HiveOptions =
      new HiveOptions(metastore, dbName, tblName(1), mapper_realtime)
        .withTxnsPerBatch(10)
        .withBatchSize(1000)
        .withIdleTimeout(10)
    val factory_realtime: StateFactory = new HiveStateFactory().withOptions(hiveOptions_realtime)


    val mapper_active: DelimitedRecordHiveMapper  =
      new DelimitedRecordHiveMapper()
        .withColumnFields(new Fields(active_columns.asJava))
        .withTimeAsPartitionField("YYYY-MM-DD'T'HH")
    val hiveOptions_active: HiveOptions =
      new HiveOptions(metastore, dbName, tblName(2), mapper_active)
        .withTxnsPerBatch(10)
        .withBatchSize(1000)
        .withIdleTimeout(10)
    val factory_active: StateFactory = new HiveStateFactory().withOptions(hiveOptions_active)


    //KafkaSpout
    val spoutConf = new TridentKafkaConfig(zkHosts_2, "air_traffic")
    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme())
    val kafkaSpout = new TransactionalTridentKafkaSpout(spoutConf)


    //Topology
    val topology: TridentTopology = new TridentTopology


    val stream: trident.Stream = topology.newStream("jsonEmitter", kafkaSpout)
      .each(new Fields("str"), new ParseJSON , new Fields(json_fields.asJava))
      .each(new Fields(json_fields.asJava), idLookup, new Fields("id"))

    stream.partitionPersist(factory_daily, new Fields(daily_columns.asJava), new HiveUpdater(), new Fields()).parallelismHint(8)
    val state = stream.partitionPersist(factory_realtime, new Fields(realtime_columns.asJava), new HiveUpdater(), new Fields()).parallelismHint(8)
    stream.partitionPersist(factory_active, new Fields(active_columns.asJava), new HiveUpdater(), new Fields()).parallelismHint(8)

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
