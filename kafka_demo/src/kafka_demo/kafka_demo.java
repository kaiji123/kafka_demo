package kafka_demo;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
//reference kafka libraries from kafka directory so that you can import functions
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
public class kafka_demo {


        public static void main(String[] args) throws Exception {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> textLines = builder.stream("quickstart-events");
            KTable<String, Long> wordCounts = textLines
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count();
            wordCounts.toStream().to("Words");

            KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.start();
            /*
        	StreamsBuilder builder = new StreamsBuilder();
        	KStream<String, String> textLines = builder.stream("quickstart-events");

        	KTable<String, Long> wordCounts = textLines
        	            .flatMapValues(line -> Arrays.asList(line.toLowerCase().split(" ")))
        	            .groupBy((keyIgnored, word) -> word)
        	            .count();
        	

        	// this writes to the topic output topic the wordcount of quickstart events
        	wordCounts.toStream().to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
        	builder.build();
        	*/
        	
            Properties props2 = new Properties();
            props2.put("bootstrap.servers", "localhost:9092");
            props2.put("group.id", "test");
            props2.put("enable.auto.commit", "true");
            props2.put("auto.commit.interval.ms", "1000");
            props2.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props2.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
            
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props2);
            consumer.subscribe(Arrays.asList("Words"));
          
            
            while(true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
	            for (ConsumerRecord<String, String> record : records) {
	                 System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	            
	            }
            }
            
        
    }
}
