package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    /*
      Connect to Cassandra and make a keyspace and table as explained in the document
    */
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    // create avg_space keyspace - create an avg table with word and count columns - and finally close session
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")
    session.close()

    /*
      Make a connection to Kafka and read (key, value) pairs from it
    */

    // define spark configuration by setting the app name and the number of local threads
    val sparkConf = new SparkConf()
      .setAppName("KafkaSpark")
      .setMaster("local[2]")
    // define kafka configuration
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")

    // create a streaming context with a 1 second batch size - create a checkpoint
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(".")
    // set the name of the topic to subscribe for new messages
    val topic = Set("avg")
    // define the receiver-less direct approach for receiving data from Kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topic)
    // extract the (key,value) pairs from messages
    val pairs = messages.map(_._2.split(",")).map( v => (v(0), v(1).toInt) )

    /*
      Measure the average value for each key in a stateful manner
    */
    val mappingFunc = (key: String, value: Option[Int], state: State[(Int, Float)]) => {
      // check whether we have previous state
      if (state.exists()) {
        // case we actually have previous state
        // increase counter of key occurrences by 1
        val counter = state.get()._1 + 1
        // according to the formula given here for the average:
        // https://math.stackexchange.com/questions/106313/regular-average-calculated-accumulatively
        val averageValue = ((counter - 1) * state.get()._2 + value.getOrElse(0)) / counter
        state.update((counter, averageValue)) // update the state to the new one
        (key, averageValue) // return new (key, value) pair
      } else {
        // initial case where no previous state has occurred
        // set the counter of key occurrence to 1
        val counter = 1
        val averageValue = value.getOrElse(0) // initially the avg is equal to the first value
        state.update((counter, averageValue)) // update the state to the new one
        (key, averageValue) // return new (key, value) pair
      }
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
  }
}
