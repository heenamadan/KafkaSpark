package com.SparkBasics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FileToRddExample {

    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Read Multiple Text Files to RDD")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // provide text file paths to be read to RDD, separated by comma
        String files = "/Users/heena.madan/Documents/projects/KafkaSpark/tweets.txt,/Users/heena.madan/Documents/projects/KafkaSpark/testfile.txt";

        // read text files to RDD
        JavaRDD<String> lines = sc.textFile(files);

        // collect RDD for printing
        for(String line:lines.collect()){
            System.out.println(line);
        }
    }
}


