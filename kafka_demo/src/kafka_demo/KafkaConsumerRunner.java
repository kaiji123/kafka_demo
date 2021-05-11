package kafka_demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class KafkaConsumerRunner implements Runnable {
	     private final AtomicBoolean closed = new AtomicBoolean(false);
	     private final KafkaConsumer consumer;

	     public KafkaConsumerRunner(Consumer<Long, String> consumer) {
	       this.consumer = (KafkaConsumer) consumer;
	     }

	     @Override
	     public void run() {
	         try {
	             consumer.subscribe(Arrays.asList("output-topic"));
	             while (!closed.get()) {
	                 ConsumerRecords records = consumer.poll(Duration.ofMillis(10000));
	                 // Handle new records
	             }
	         } catch (WakeupException e) {
	             // Ignore exception if closing
	             if (!closed.get()) throw e;
	         } finally {
	             consumer.close();
	         }
	     }

	     // Shutdown hook which can be called from a separate thread
	     public void shutdown() {
	         closed.set(true);
	         consumer.wakeup();
	     }
	     public static void main(String[] args) {
	    	 Consumer<Long, String> consumer = KafkaConsumerExample.createConsumer();
	    	 KafkaConsumerRunner b = new KafkaConsumerRunner(consumer);
	    	 Thread thr= new Thread(b);
	    	 thr.start();
	     }
	 
}
