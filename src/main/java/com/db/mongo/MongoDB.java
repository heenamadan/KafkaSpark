package com.db.mongo;

import com.model.Emoji;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

import java.net.UnknownHostException;
import java.util.Objects;


public class MongoDB {


    // Utility method to get database instance
    private static DB getDB() {
        MongoClient mClient = MongoSingleTon.getInstance();
        return  mClient.getDB("twitter-data");

    }



    public static DBCollection getCollection(){
        return getDB().getCollection("emoji");
    }

    public static DBCollection getTweetCollection(){
        return getDB().getCollection("tweets");
    }

    public static DBCollection getHashCollection(){
        return getDB().getCollection("hashtags");
    }

    public static DBCollection getAtTheRateCollection(){
        return getDB().getCollection("atrate");
    }

    public static void insertTweets(String tweet){
        DBCollection collection = getTweetCollection();
        collection.insert(createTweetDBObject(tweet));
    }


    public static void insertOrUpdate(Emoji emModel){




        DBCollection collection = getCollection();

        BasicDBObject updateDocument = new BasicDBObject();


        BasicDBObject searchQuery = new BasicDBObject().append("emoji", emModel.getEmoji());

        DBObject foundDoc  = collection.findOne(new BasicDBObject("emoji", emModel.getEmoji()));
        if(Objects.nonNull(foundDoc)) {
            updateDocument.append("$set", new BasicDBObject().append("count", Long.parseLong(foundDoc.get("count").toString())+1));
            collection.update(searchQuery, updateDocument);
        } else {

            collection.insert(createEmojiDBObject(emModel));

        }
        //WriteResult result = collection.remove(new BasicDBObject());

        DBCursor cursor = collection.find();
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }



        // insert(emoji,count,collection);

    }

    public static void insertOrUpdateHash(Emoji emModel, String what){

        DBCollection collection;

        if("hash".equalsIgnoreCase(what)) {
             collection = getHashCollection();

        } else {
            collection = getAtTheRateCollection();
        }


        BasicDBObject updateDocument = new BasicDBObject();


        BasicDBObject searchQuery = new BasicDBObject().append("emoji", emModel.getEmoji());

        DBObject foundDoc  = collection.findOne(new BasicDBObject("emoji", emModel.getEmoji()));
        if(Objects.nonNull(foundDoc)) {
            updateDocument.append("$set", new BasicDBObject().append("count", Long.parseLong(foundDoc.get("count").toString())+1));
            collection.update(searchQuery, updateDocument);
        } else {

            collection.insert(createEmojiDBObject(emModel));

        }
        //WriteResult result = collection.remove(new BasicDBObject());

        DBCursor cursor = collection.find();
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }



        // insert(emoji,count,collection);

    }
    private static DBObject createEmojiDBObject(Emoji emoji) {
        BasicDBObjectBuilder docBuilder = BasicDBObjectBuilder.start();


        docBuilder.append("emoji", emoji.getEmoji());
        docBuilder.append("count", emoji.getCount());
        //docBuilder.append("count", emoji.getCount());
        return docBuilder.get();
    }

    private static DBObject createTweetDBObject(String tweet) {
        BasicDBObjectBuilder docBuilder = BasicDBObjectBuilder.start();


        docBuilder.append("tweet", tweet);
        return docBuilder.get();
    }

    public static void main(String[] args) throws UnknownHostException
    {

        ///Delete All documents before running example again
        // WriteResult result = collection.remove(new BasicDBObject());
        //System.out.println(result.toString());

        Emoji emModel= new Emoji();
        emModel.setEmoji("\uD83D\uDC7F");
        emModel.setCount(1);


        // insert1(createDBObject(emModel), collection);


        insertOrUpdate(emModel);



    }




}
