package com.github.personal.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoSeekAssign {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Logger logger = LoggerFactory.getLogger(ConsumerDemoSeekAssign.class);
		
		String bootstrapServer = "127.0.0.1:9092";
		String topic = "first_topic";
		//String groupId = "my_fourth_application";
		
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //Subscribe to a topic
        
        //kafkaConsumer.subscribe(Collections.singleton(topic));

        //Partition to read from
        TopicPartition partition = new TopicPartition(topic,0);
        
        long offsetToReadFrom = 4L;
        
       
        //Assign
        kafkaConsumer.assign(Arrays.asList(partition));
        
        //seek
        kafkaConsumer.seek(partition, offsetToReadFrom);
        
        boolean keepOnReading = true;
        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        
        //Poll for new data 
        while(keepOnReading) {
            ConsumerRecords<String, String> records =  kafkaConsumer.poll(Duration.ofMillis(100));
            
            for(ConsumerRecord<String,String> record : records){
            	numberOfMessagesReadSoFar +=1;
            	logger.info("Key: "+ record.key() + ", Value: "+ record.value());
            	logger.info("Partition: "+ record.partition() + ", Offset: "+ record.offset());
            	if(numberOfMessagesReadSoFar == numberOfMessagesToRead) {
            		keepOnReading = false;
            		break;
            	}
            }
            
         

        }
        
        
	}

}
