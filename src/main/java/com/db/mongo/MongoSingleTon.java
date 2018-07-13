package com.db.mongo;

import com.mongodb.MongoClient;

public class MongoSingleTon {

    private static MongoClient instance;

    public static MongoClient getInstance(){
        if(instance == null ) {
            instance= new MongoClient();
        }
        return instance;
    }


    private MongoSingleTon(){
        System.out.println("my private default contructor");
    }
}
