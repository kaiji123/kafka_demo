package kafka_demo;

import java.util.Arrays;
import java.util.Properties;
//run consumer file first then producer!!!!!
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class consumers {
	@SuppressWarnings("deprecation")
	public static void main(String args[]) {
		 Properties props = new Properties();

		    props.put("bootstrap.servers", "localhost:9092");
		  
		    props.put("group.id", "asst");
		      props.put("enable.auto.commit", "true");
		        props.put("auto.commit.interval.ms", "1000");
		        props.put("auto.offset.reset", "earliest");
		        props.put("session.timeout.ms", "30000");

		    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		    KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
		    consumer.subscribe(Arrays.asList("my-topic"));
		  
		    
		    while(true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
			
		        for (ConsumerRecord<String, String> record : records) {
		        	System.out.println(record);
		             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		        
		        }
		        consumer.commitAsync();
		    }
		    }
	



    


}
