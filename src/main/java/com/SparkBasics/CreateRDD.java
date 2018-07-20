package com.SparkBasics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class CreateRDD {

    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD foreach Example")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // read list to RDD
        List<String> data = Arrays.asList("Learn","Apache","Spark","with","Tutorial Kart");
        JavaRDD<String> items = sc.parallelize(data,1);
        System.out.println("Number of partitions-->"+items.getNumPartitions());

        // apply a function for each element of RDD
        items.foreach(item -> {
            System.out.println("* "+item);
        });

        //IntStream.range(0, data.size()).mapToObj(index->String.format("%d->%s", index,))
    }
}
