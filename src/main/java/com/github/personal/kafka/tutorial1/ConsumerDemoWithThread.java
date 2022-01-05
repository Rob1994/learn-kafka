package com.github.personal.kafka.tutorial1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
		
		String bootstrapServer = "127.0.0.1:9092";
		String topic = "first_topic";
		String groupId = "my_third_application_1";
	
		CountDownLatch latch = new CountDownLatch(1);
		//Consumer created
		logger.info("Consumer is creating...");
		Runnable consumerThread = new ConsumerThread(bootstrapServer, groupId,topic,latch);
		
		Thread thread = new Thread(consumerThread);
		thread.start();
		
		
		//Add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("Caught Shutdownhook");
			((ConsumerThread)consumerThread).shutDown();
			
			try {
				logger.info("Application has started closing");

				latch.await();
			} catch (InterruptedException e) {
				
				logger.info("Application is closed");
			}
			
		}));
		
		
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.info("Application got interrupted", e);
		}finally {
			logger.info("Application is closing");

		}
		
		
        
	}
	
	


}


class ConsumerThread implements Runnable {
	
	Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

	
	private KafkaConsumer<String, String> kafkaConsumer;
	private CountDownLatch latch;
	
	public ConsumerThread(String bootstrapServer, String groupId, String topic, CountDownLatch latch){
		
		this.latch = latch;
		
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        //Create Consumer
        kafkaConsumer = new KafkaConsumer<>(properties);
        
        //Subscribe to a topic
        kafkaConsumer.subscribe(Collections.singleton(topic));

	}
	

	@Override
	public void run() {
        //Poll for new data 
        try {
			while(true) {
			    ConsumerRecords<String, String> records =  kafkaConsumer.poll(Duration.ofMillis(100));
			    
			    records.forEach(record->{
			    	logger.info("Key: "+ record.key() + ", Value: "+ record.value());
			    	logger.info("Partition: "+ record.partition() + ", Offset: "+ record.offset());
			    });
			    

			}
		} catch (WakeupException e) {
			logger.info("Received Shutdown signal");
		} finally {
			kafkaConsumer.close();
			//notify the main code that the consumer has ended
			latch.countDown();
		}
		
	}
	
	public void shutDown() {
		
		//special wake up() method to interrupt kafkaconsumer.poll() method
		//throws an exception WakeUpException
		kafkaConsumer.wakeup();
	}
	
	
	
	
}
