package com.apache.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AppConsumer {
	
	public static void main(String[] args) {
		
		String topicName ="my_message";
		String groupName ="my_message_group";
		
		Properties prop = new Properties();
		prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092" );
		prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
			
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(prop);
		kafkaConsumer.subscribe(Arrays.asList(topicName));
		
		while(true) {
			for(ConsumerRecord<String, String> record : kafkaConsumer.poll(Duration.ofMillis(100))) {
				System.out.println("Message Received > "+ String.valueOf(record.value()));
			}
		}
		
	}
}
