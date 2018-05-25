package com.pattersonconsultingtn.kafka.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
 
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
 
 /**

	If we follow the Apache Kafka Quickstart too closely, we end up with this issue for the pipe demo (doesnt have a key like the wordcount):

	https://stackoverflow.com/questions/49098274/kafka-stream-get-corruptrecordexception

	Updates:

	(1) topic creation for input

	bin/kafka-topics.sh --create \
	    --zookeeper localhost:2181 \
	    --replication-factor 1 \
	    --partitions 1 \
	    --topic streams-plaintext-input

	(2) topic creation for output

	bin/kafka-topics.sh --create \
	--zookeeper localhost:2181 \
	--replication-factor 1 \
	--partitions 1 \
	--topic streams-pipe-output


	(3) kafka producer setup from console

	bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-plaintext-input

	(4) kafka consumer setup from console

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic streams-pipe-output \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

 */
public class Pipe {
 
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
 
        final StreamsBuilder builder = new StreamsBuilder();
 
        builder.stream("streams-plaintext-input").to("streams-pipe-output");
 
        final Topology topology = builder.build();
 
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
 
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
            	System.out.println( "Shutting Down, closing streams..." );
                streams.close();
                latch.countDown();
            }
        });
 
        try {
        	System.out.println( "Starting streams..." );
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}