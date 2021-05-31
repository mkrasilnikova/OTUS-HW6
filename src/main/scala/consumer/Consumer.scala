package consumer


import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.{IterableHasAsScala, SeqHasAsJava}


object Consumer extends App {

  val topicName = "books"
  val props = buildProperties
  val consumer = new KafkaConsumer[String, String](props)

  try {
    setOffsets
    while (true) {
      println("Polling")
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1))
      for (record <- records.asScala) {
        val value = record.value()
        val offset = record.offset()
        print(record.partition() + "_______" + offset + "_______")
        println(value)
      }
    }
  } finally {
    consumer.close()
  }

  def buildProperties(): Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties
  }

  def getPartitions(topic: String, clusterConfig: Properties) = {
    val admin = Admin.create(clusterConfig)
    val topicDesc = admin.describeTopics(Collections.singletonList(topic)).values().get("books").get()
    topicDesc.partitions().asScala
      .map(partitionInfo => new TopicPartition(topicName, partitionInfo.partition()))
      .toList
  }

  private def setOffsets = {
    val partitions = getPartitions(topicName, props).asJava
    consumer.assign(partitions)
    consumer.seekToEnd(partitions)

    for (partition <- partitions.asScala) {
      val lastMessagePosition = consumer.position(partition)
      println("lastMessagePosition  " + lastMessagePosition + " for partition " + partition.partition())
      consumer.seek(partition, lastMessagePosition - 5)
    }
  }
}
