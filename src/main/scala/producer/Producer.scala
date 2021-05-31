package producer

import io.circe._
import io.circe.generic.auto._
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.errors.TopicExistsException

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Try

object Producer extends App {

  case class BookInfo(
                       name: String,
                       author: String,
                       userRating: Double,
                       reviews: String,
                       price: Int,
                       year: Int,
                       genre: String
                     )

  val props = buildProperties()
  val topicName = "books"
  createTopic(topicName, props)
  val producer = new KafkaProducer[String, String](props)

  import org.apache.commons.csv.{CSVFormat, CSVParser}

  try {
    val csvData: InputStream = getClass().getClassLoader().getResourceAsStream("bestsellers_with_categories-1801-9dc31f.csv");
    val parser = CSVParser.parse(csvData,
      StandardCharsets.UTF_8,
      CSVFormat.RFC4180
        .withHeader("Name", "Author", "User Rating", "Reviews", "Price", "Year", "Genre")
        .withSkipHeaderRecord)

    for (record <- parser.asScala) {
      val bookInfo = BookInfo(record.get("Name"), record.get("Author"), record.get("User Rating").toDouble,
        record.get("Reviews"), record.get("Price").toInt, record.get("Year").toInt, record.get("Genre"))

      val json: Json = Encoder[BookInfo].apply(bookInfo)
      println(json.noSpaces)
      println()
      val record1 = new ProducerRecord[String, String](topicName, null, json.noSpaces)
      producer.send(record1)
    }
    producer.flush()
  } finally {
    producer.close()
  }

  def buildProperties(): Properties = {
    val properties: Properties = new Properties
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties
  }

  def createTopic(topic: String, clusterConfig: Properties): Unit = {
    val newTopic = new NewTopic(topic, 3, 1.toShort)
    val admin = Admin.create(clusterConfig)
    Try(admin.createTopics(Collections.singletonList(newTopic)).all.get).recover {
      case e: Exception =>
        println(e)
        // Ignore if TopicExistsException, which may be valid if topic exists
        if (!e.getCause.isInstanceOf[TopicExistsException]) throw new RuntimeException(e)
    }
    admin.close()
  }
}
