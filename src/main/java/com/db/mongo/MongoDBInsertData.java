package com.db.mongo;

import com.model.Emoji;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;


import java.net.UnknownHostException;
import java.util.Objects;

public class MongoDBInsertData
{


    public static DBCollection getCollection(){

        MongoClient mongo = new MongoClient("localhost", 27017);
        DB db = mongo.getDB("twitter-data");
        return db.getCollection("mycollection1");
    }


    public void insertOrUpdate(Emoji emModel){

        DBCollection collection = getCollection();

        BasicDBObject updateDocument = new BasicDBObject();


        BasicDBObject searchQuery = new BasicDBObject().append("emoji", emModel.getEmoji());

        DBObject foundDoc  = collection.findOne(new BasicDBObject("emoji", emModel.getEmoji()));
        if(Objects.nonNull(foundDoc)) {
            updateDocument.append("$set", new BasicDBObject().append("count", Long.parseLong(foundDoc.get("count").toString())+1));
            collection.update(searchQuery, updateDocument);
        } else {

            collection.insert(createDBObject(emModel));

        }

        DBCursor cursor = collection.find();
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }

    }

    private static DBObject createDBObject(Emoji emoji) {
        BasicDBObjectBuilder docBuilder = BasicDBObjectBuilder.start();


        docBuilder.append("emoji", emoji.getEmoji());
        docBuilder.append("count", emoji.getCount());
        return docBuilder.get();
    }

    private static void insert1(DBObject dbObject, DBCollection collection){
        collection.insert(dbObject);

    }

    private static void insert(String emoji, long count, DBCollection collection){

        BasicDBObject document = new BasicDBObject();
        Emoji emModel= new Emoji();
        emModel.setCount(count);
        emModel.setEmoji(emoji);
        document.put(emoji, count);
        collection.insert(document);


    }
}
