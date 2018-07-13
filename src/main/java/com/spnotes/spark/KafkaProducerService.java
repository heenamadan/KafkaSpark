package com.spnotes.spark;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerService {
    private  final String topic ;
    kafka.javaapi.producer.Producer<String, Object> producer ;
    Producer<String, String> producer1;

    public KafkaProducerService(String topic1){
        topic= topic1;
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        ProducerConfig producerConfig = new ProducerConfig(properties);
        producer = new kafka.javaapi.producer.Producer<String, Object>(
                producerConfig);
         //producer1 = new KafkaProducer<String, String>(properties);


    }

    public void sendMessage(String result, String result1){
        //producer1.send(new ProducerRecord<String, String>("test", result, result1));


       KeyedMessage<String, Object> km = new KeyedMessage<String, Object>(topic, result);
        producer.send(km);

    }

}