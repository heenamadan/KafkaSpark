package com.spnotes.spark;

import com.db.mongo.MongoDB;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.model.Emoji;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Serializable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HashTagExtractor implements Serializable {
    public static final long UID = 1L;
    static MongoDB mongoDBInsertData = new MongoDB();

    public static void main(String[] args) throws InterruptedException, IOException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HashTagExtractor");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(30));

        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group3");
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

        //filter hashtags
        JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
            public Boolean call(String word) throws Exception {
                return word.startsWith("#");
            }
        });


        //filter @
       JavaDStream<String> atTheRates = words.filter(new Function<String, Boolean>() {
            public Boolean call(String word) throws Exception {
                return word.startsWith("@");
            }
        });

        hashTags.foreachRDD(kafkastream->printRDD(kafkastream, "hash"));

        atTheRates.foreachRDD(kafkastream->printRDD(kafkastream,"at"));

        //hashTags.dstream().saveAsTextFiles("/Users/heena.madan/Downloads/KafkaSpark10-master/hashtags", "txt");
        //atTheRates.dstream().saveAsTextFiles("/Users/heena.madan/Downloads/KafkaSpark10-master/atrate", "txt");

       // perform(hashTags);

        ssc.start();
        ssc.awaitTermination();
    }

    public static void printRDD(final JavaRDD<String> s, String what) throws IOException{
// s.foreach(System.out::println);

        for(String hash: s.collect()) {
            System.out.println(hash);
            Emoji emModel= new Emoji();
            emModel.setCount(1);
            emModel.setEmoji(hash);
            mongoDBInsertData.insertOrUpdateHash(emModel, what);
        }


    }

    private static void countEmoji(String str, LinkedHashMap<String,Integer> map) {
        System.out.println("str-->"
                +str);





        List<String> lists=EmojiUtils.extractEmojisAsString(str);
        for(String emo: lists) {
            System.out.println("emoji");
            System.out.println(emo);
            Emoji emModel= new Emoji();
            emModel.setCount(1);
            emModel.setEmoji(emo);
            mongoDBInsertData.insertOrUpdate(emModel);
        }
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
            String key = "\"" + word + "\"";
            String val = "\"" + count + "\"";
            String time = "\"" + new Date() + "\"";
            String event1 = "[{\"key\":" + key + "}, {\"value\":" + val + "}, {\"fetchTime\":" + time + "}]";
            producer.send(new ProducerRecord<String, String>(topic, event1));
        } catch (Exception e) {
            System.out.println("Problem while publishing the event to kafka.."+ e.getMessage());
        }
        producer.close();
    }
}
