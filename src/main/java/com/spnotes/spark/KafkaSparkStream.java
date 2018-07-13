package com.spnotes.spark;

import com.db.mongo.MongoDB;
import com.model.Emoji;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Serializable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * This class reads data from kafka topic, Extracts emojis and save to mongodb
 *
 *
 * ***/
public class KafkaSparkStream implements Serializable{
    public static final long UID = 1L;
    static MongoDB mongoDBInsertData = new MongoDB();

    public static void main(String[] args) throws InterruptedException, IOException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("KafkaSparkStream");

        conf.set("spark.driver.allowMultipleContexts", "true");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        //javaSparkContext.
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new       Duration(30));

        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"group2");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        Collection<String> topics = Arrays.asList("hashtag-twitter-data");



        final JavaInputDStream<ConsumerRecord<String, String>> kafkaSparkPairInputDStream =
                KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams));

        JavaDStream<String> lines = kafkaSparkPairInputDStream.map(new Function<ConsumerRecord<String,String>, String>() {
            public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
                return kafkaRecord.value();
            }
        });

        LinkedHashMap<String,Integer> elementCountMap = new LinkedHashMap();
        lines.foreachRDD(kafkastream->printRDD(elementCountMap, kafkastream));




        ssc.start();
        ssc.awaitTermination();
    }


    public static void printRDD( LinkedHashMap<String,Integer> elementCountMap,final JavaRDD<String> s) throws IOException{

        for(String l: s.collect()) {
            System.out.println("mongo-->"+mongoDBInsertData);
            countEmoji(l, elementCountMap);
        }

    }


    private static void countEmoji(String str, LinkedHashMap<String,Integer> map) {
         mongoDBInsertData.insertTweets(str);
        final List<String> lists=EmojiUtils.extractEmojisAsString(str);
        for(String emo: lists) {
            System.out.println("emoji");
            System.out.println(emo);
            Emoji emModel= new Emoji();
            emModel.setCount(1);
            emModel.setEmoji(emo);
            mongoDBInsertData.insertOrUpdate(emModel);
        }
    }
}

