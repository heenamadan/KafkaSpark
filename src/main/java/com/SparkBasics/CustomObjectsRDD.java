package com.SparkBasics;

import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class CustomObjectsRDD {

    public static void main(String[] args) throws IOException{
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Print Elements of RDD")
                .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // prepare list of objects
        List<Person> personList = ImmutableList.of(
                new Person("Heena", 25),
                new Person("Madan", 2));

        // parallelize the list using SparkContext
        JavaRDD<Person> perJavaRDD = sc.parallelize(personList);

        for(Person person : perJavaRDD.collect()){
            System.out.println(person.name);
        }

        sc.close();
        //System.in.read();
    }
}

class Person implements Serializable{
    private static final long serialVersionUID = -2685444218382696366L;
    String name;
    int age;
    public Person(String name, int age){
        this.name = name;
        this.age = age;
    }
}
