package com.mongodb.m101j.hw;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;

public class hw2_3
{
    public static void main(String[] args)
    {
        MongoClient client = new MongoClient();
        MongoDatabase database = client.getDatabase("students");
        MongoCollection<Document> collection = database.getCollection("grades");

        Bson filter = eq("type", "homework");
        Bson sort = ascending("student_id", "score");

        MongoCursor<Document> cursor = collection
                .find(filter)
                .sort(sort)
                .iterator();
        try
        {
            while (cursor.hasNext())
            {
                Document lowestHomeworkScoreDocument = cursor.next();
                ObjectId lowestHomeworkScoreDocumentObjectId = lowestHomeworkScoreDocument.getObjectId("_id");

                if (cursor.hasNext())
                {
                    cursor.next();
                }

                collection.deleteOne(eq("_id", lowestHomeworkScoreDocumentObjectId));
            }
        }
        finally
        {
            cursor.close();
        }
    }
}
