package topology

import clients._
import clients.mongo.{MongoSetOnInsertMapper, MongoStateFactory, MongoStateUpdater, Options}
import kafka.api.{OffsetFetchRequest, OffsetRequest}
import org.bson.Document


/**
  * Created by Stefano on 19/07/2017.
  */
object MongoDBTopology extends App {

  import java.util

  import org.apache.storm.kafka._
  import org.apache.storm.{Config, StormSubmitter, trident}
  import org.apache.storm.trident.{TridentState, TridentTopology}
  import org.apache.storm.tuple.{Fields, Values}
  import org.apache.storm.kafka.trident.{TransactionalTridentKafkaSpout, TridentKafkaConfig}
  import org.apache.storm.spout.SchemeAsMultiScheme
  import org.apache.storm.trident.operation.BaseFunction
  import org.apache.storm.trident.operation.TridentCollector
  import org.apache.storm.trident.tuple.TridentTuple
  import org.joda.time.{DateTime, DateTimeZone}
  import play.api.libs.json.{JsValue, Json}

  import scala.collection.JavaConverters._
  import scala.collection.concurrent.TrieMap

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
        val time = tuple.getValueByField("formatted_date")

        collector.emit(new Values(new Integer(createOrGetId(key, time.asInstanceOf[util.Date].getTime))))

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
            result("origin").toString.stripPrefix("\"").stripSuffix("\""),
            result("flight").toString.stripPrefix("\"").stripSuffix("\""),
            new Integer(result("course").toString.toInt),
            result("aircraft").toString.stripPrefix("\"").stripSuffix("\""),
            result("callsign").toString.stripPrefix("\"").stripSuffix("\""),
            result("registration").toString.stripPrefix("\"").stripSuffix("\""),
            new java.lang.Double(result("lat").toString.toDouble),
            new Integer(result("speed").toString.toInt),
            new Integer(result("altitude").toString.toInt),
            result("destination").toString.stripPrefix("\"").stripSuffix("\""),
            new java.lang.Double(result("lon").toString.toDouble),
            new util.Date(result("time").toString.toLong*1000),
            new util.Date(result("time").toString.toLong*1000),
            new util.Date(result("time").toString.toLong*1000)))
        } catch {

          case e: Exception => println("Error parsing: " + json_string); println(e)

        }
      }
    }

    override def main(args: Array[String]): Unit = {

      //Storm +  Kafka
      val master_1 = "master-1.localdomain"
      val master_2 = "master-2.localdomain"
      val zkHosts_2 = new ZkHosts(master_2 + ":2181")

      //MongoDB
      val dbName = "DataStream"
      val mongoURL = "mongodb://10.0.0.80:30003/" + dbName
      val active_collection = "activeCollection"
      val history_collection = "history"
      val json_fields = Seq("origin", "flight", "course", "aircraft", "callsign",
        "registration", "lat", "speed", "altitude", "destination", "lon") //time

      val history_columns = Seq("_id", "origin", "flight", "course", "aircraft", "callsign",
        "registration",  "destination", "date_depart", "date_arrival")
      val active_columns = Seq("_id", "origin", "destination", "lat", "lon", "formatted_date", "aircraft", "speed", "course")

      //MongoDBConnector
      val active_mapper = new MongoSetOnInsertMapper()
          .withSetFields(List("lat", "lon", "speed", "course", "formatted_date" ))
          .withSetOnInsertFields(List("_id", "origin", "destination", "aircraft"))

      val history_mapper = new MongoSetOnInsertMapper()
        .withSetFields(List("date_arrival"))
        .withSetOnInsertFields(List("origin", "destination", "aircraft", "flight", "registration", "callsign",  "date_depart"))

      val active_options = new Options()
        .withUrl(mongoURL)
        .withCollectionName(active_collection)
        .withMapper(active_mapper)

      val history_options = new Options()
        .withUrl(mongoURL)
        .withCollectionName(history_collection)
        .withMapper(history_mapper)


      val active_factory = new MongoStateFactory(active_options)
      val history_factory = new MongoStateFactory(history_options)

      //KafkaSpout
      val spoutConf = new TridentKafkaConfig(zkHosts_2, "air_traffic2", "air_traffic_consumer")
      spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme())
      spoutConf.fetchSizeBytes = 1024*256
      val kafkaSpout = new TransactionalTridentKafkaSpout(spoutConf)


      //Topology
      val topology: TridentTopology = new TridentTopology

      val stream: trident.Stream = topology.newStream("jsonEmitter", kafkaSpout)
        .each(new Fields("str"), new ParseJSON , new Fields((((json_fields :+ "formatted_date") :+ "date_depart") :+ "date_arrival").asJava))
        .each(new Fields((json_fields :+ "formatted_date").asJava), idLookup, new Fields("_id"))

      stream.partitionPersist(active_factory, new Fields(active_columns.asJava), new MongoStateUpdater(), new Fields()).parallelismHint(15)

      stream.partitionPersist(history_factory, new Fields(history_columns.asJava), new MongoStateUpdater(), new Fields()).parallelismHint(15)

      //Storm Config
      val config = new Config
      config.setNumAckers(1)
      config.setMaxTaskParallelism(15)
      config.setMaxSpoutPending(1500)
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
