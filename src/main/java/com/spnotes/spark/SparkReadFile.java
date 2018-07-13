package com.spnotes.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
/**
 *
 * This class  reads file and extract emojis and saves to file
 *
 * ***/
public class SparkReadFile {
    static LinkedHashMap<String,Integer> elementCountMap = new LinkedHashMap();
    public static void main(String[] args) throws InterruptedException, IOException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(" ");

        JavaSparkContext ssc = new JavaSparkContext(sparkConf);


        JavaRDD<String> ifiledata = ssc.textFile("/Users/heena.madan/Downloads/KafkaSpark10-master/countemoji.txt");



       for(String l: ifiledata.collect()) {
           countEmoji(l);

       }

    }

    private static void countEmoji(String str) throws IOException{
        List<String> lists=EmojiUtils.extractEmojisAsString(str);
        for(String l: lists) {
            System.out.println(l);
              writeToFile1(l);
            createEmojiCountMap(l,elementCountMap);
            System.out.println("size--->"+elementCountMap.size());

        }

    }


    public static void printRDD(final JavaRDD<String> s) throws IOException{
        for(String l: s.collect()) {
            countEmoji(l);

        }
    }

    private static void createEmojiCountMap(String emoji, LinkedHashMap<String,Integer> elementCountMap) {
        //System.out.println(emoji + "-----"+elementCountMap.size());

        if (elementCountMap.containsKey(emoji))
        {
            //If an element is present, incrementing its count by 1

            elementCountMap.put(emoji, elementCountMap.get(emoji)+1);
        }
        else
        {
            //If an element is not present, put that element with 1 as its value

            elementCountMap.put(emoji, 1);
        }

    }


    private static void writeToFile1(String emo) throws IOException {
        File f1 = new File("/Users/heena.madan/Downloads/KafkaSpark10-master/heena.txt");
        if(!f1.exists()) {
            f1.createNewFile();
        }
        //FileWriter fileWritter = new FileWriter(f1);
        FileWriter fileWritter = new FileWriter(f1.getName(),true);
        BufferedWriter bw = new BufferedWriter(fileWritter);
        bw.write(emo);

    }
}
