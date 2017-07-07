package runtime

import java.util.Properties

object Main extends App {

  import org.apache.kafka.clients.consumer.KafkaConsumer
  import java.util
  import scala.collection.JavaConversions._


  val props = new Properties()
  props.put("bootstrap.servers", "master-1.localdomain:9092")
  props.put("group.id", "test")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Arrays.asList("air_traffic"))
  while (true) {
    val records = consumer.poll(100)
    for (record <- records) {
      System.out.println("offset = %d, key = %s, value = %s", record.offset, record.key, record.value)
    }
  }
}
