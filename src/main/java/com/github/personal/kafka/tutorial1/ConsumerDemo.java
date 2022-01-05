package com.github.personal.kafka.tutorial1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		
		String bootstrapServer = "127.0.0.1:9092";
		String topic = "first_topic";
		String groupId = "my_fourth_application";
		
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //Subscribe to a topic
        
        kafkaConsumer.subscribe(Collections.singleton(topic));
        
        //Poll for new data 
        while(true) {
            ConsumerRecords<String, String> records =  kafkaConsumer.poll(Duration.ofMillis(100));
            
            records.forEach(record->{
            	logger.info("Key: "+ record.key() + ", Value: "+ record.value());
            	logger.info("Partition: "+ record.partition() + ", Offset: "+ record.offset());
            });
            
         

        }
        
        
	}

}
