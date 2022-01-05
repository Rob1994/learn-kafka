package com.github.personal.kafka.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    public static void main(String[] args) {
    	
    	Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
    	
        System.out.println("Hello World..");

        String bootstrapServer = "127.0.0.1:9092";
        //Create the Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        
        
        for(int i=0 ; i<10; i++) {

        //Create a Producer Record to be sent to Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic","Hello World new from sts" + Integer.toString(i));

        //Send Data --async
        
        
        producer.send(record, new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				
				if(exception == null) {
					
					LOGGER.info("Received new Metadata: \n" + 
					"Topic: "+ metadata.topic() + "\n" +
					"Offset: "+ metadata.offset()+ "\n" +
					"Partition: "+ metadata.partition()+ "\n" +
					"TimeStamp: "+ metadata.timestamp() +"\n");
					
				} else {
					LOGGER.error("Exception Occured:",exception);
				}
			}
		});
        }

        //flush data
        producer.flush();

        //flush and close
        producer.close();
    }
}