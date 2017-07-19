package topology

import clients.{MongoStateFactory, MongoStateUpdater, Options}

/**
  * Created by Stefano on 19/07/2017.
  */
object MongoDBTopology extends App {

  import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper
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
  import scala.collection.concurrent
  import scala.collection.concurrent.TrieMap
  import scala.collection.mutable

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

    class DateCreation extends BaseFunction {

      override final def execute(tuple: TridentTuple, collector: TridentCollector): Unit = {

        collector.emit(new Values(DateTime.now(DateTimeZone.UTC).toString("YYYY-MM-DD'T'hh:mm:ss")))

      }


    }

    class DepartureArrivalDates extends BaseFunction {

      override final def execute(tuple: TridentTuple, collector: TridentCollector): Unit = {

        collector.emit(new Values(DateTime.now(DateTimeZone.UTC).toString("YYYY-MM-DD'T'hh:mm:ss"),
                        DateTime.now(DateTimeZone.UTC).toString("YYYY-MM-DD'T'hh:mm:ss")))

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

      val history_columns = Seq("id", "origin", "flight", "course", "aircraft", "callsign",
        "registration",  "destination", "date_depart", "date_arrival")
      val active_columns = Seq("id", "origin", "destination", "lat", "lon", "formatted_date", "aircraft", "speed", "course")

      //MongoDBConnector
      val active_mapper = new SimpleMongoMapper().withFields("id", "origin", "destination", "lat", "lon",
        "formatted_date", "aircraft", "speed", "course")

      val history_mapper = new SimpleMongoMapper().withFields("id", "origin", "flight", "course", "aircraft", "callsign",
        "registration",  "destination", "date_depart", "date_arrival")

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
      val spoutConf = new TridentKafkaConfig(zkHosts_2, "air_traffic")
      spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme())
      val kafkaSpout = new TransactionalTridentKafkaSpout(spoutConf)


      //Topology
      val topology: TridentTopology = new TridentTopology


      val stream: trident.Stream = topology.newStream("jsonEmitter", kafkaSpout)
        .each(new Fields("str"), new ParseJSON , new Fields((json_fields :+ "time").asJava))
        .each(new Fields(), new DateCreation, new Fields("formatted_date"))
        .each(new Fields((json_fields :+ "time").asJava), idLookup, new Fields("id"))


      stream.partitionPersist(active_factory, new Fields(active_columns.asJava ), new MongoStateUpdater(), new Fields()).parallelismHint(8)
      stream.each(new Fields(), new DepartureArrivalDates(), new Fields("date_depart", "date_arrival"))
        .partitionPersist(history_factory, new Fields(history_columns.asJava), new MongoStateUpdater(), new Fields()).parallelismHint(8)

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
