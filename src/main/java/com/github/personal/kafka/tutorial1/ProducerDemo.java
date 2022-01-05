package com.github.personal.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello World..");

        String bootstrapServer = "127.0.0.1:9092";
        //Create the Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create a Producer Record to be sent to Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic","Hello World new from sts");

        //Send Data --async
        producer.send(record);

        //flush data
        producer.flush();

        //flush and close
        producer.close();
    }
}