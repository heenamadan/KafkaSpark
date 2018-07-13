package com.spnotes.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class WordCount {

    public static void main(String[] args) throws InterruptedException, IOException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("KafkaSparkStream");
        //JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(30));

        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        Collection<String> topics = Arrays.asList("hashtag-twitter-data");


        final JavaInputDStream<ConsumerRecord<String, String>> kafkaSparkPairInputDStream =
                KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        JavaDStream<String> lines = kafkaSparkPairInputDStream.map(new Function<ConsumerRecord<String, String>, String>() {
            public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
                return kafkaRecord.value();
            }
        });

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        perform(wordCounts);
    }

    private static void perform(JavaPairDStream<String, Integer> wordCounts){
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        System.out.println("in perform");
        try {
            /*wordCounts.foreachRDD(rdd -> {
                rdd.foreachPartition(iter ->{
                    KafkaProducerService kafkaProducer  = new KafkaProducerService("javaworld");
                    while (iter.hasNext()){
                        kafkaProducer.sendMessage(iter.next()._1, iter.next()._2.toString());
                    }
                });

            });*/

            wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
                @Override
                public void call(JavaPairRDD<String, Integer> arg0) throws Exception {
                    Map<String, Integer> wordCountMap = arg0.collectAsMap();

                    for (String key : wordCountMap.keySet()) {
                        //Here we send event to kafka output topic
                        publishToKafka(key, new Long(wordCountMap.get(key)), "test2",props);
                    }

                }
            });


            System.out.println("added to topic");
        } catch (Exception ex) {
            ex.printStackTrace();

        }
    }


    public static void publishToKafka(String word, Long count, String topic, Properties props) {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        try {
            ObjectMapper mapper = new ObjectMapper();
            //String jsonInString = mapper.writeValueAsString(word + "," + count);
            String key = "\"" + word + "\"";
            String val = "\"" + count + "\"";
            String time = "\"" + new Date() + "\"";
            //String event = "{\"word_stats\":" + jsonInString + "}";
            String event1 = "[{\"key\":" + key + "}, {\"value\":" + val + "}, {\"fetchTime\":" + time + "}]";
            // log.info("Message to send to kafka : {}", event);
            producer.send(new ProducerRecord<String, String>(topic, event1));
            //log.info("Event : " + event + " published successfully to kafka!!");
        } catch (Exception e) {
            System.out.println("Problem while publishing the event to kafka.."+ e.getMessage());
            // log.error("Problem while publishing the event to kafka : " + e.getMessage());
        }
        producer.close();
    }
    }
