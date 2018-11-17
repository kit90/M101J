package com.mongodb.m101j.hw;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Iterator;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.pull;

public class hw3_1 {
    public static void main(final String[] args) {
        MongoClient client = new MongoClient();
        MongoDatabase numbersDB = client.getDatabase("school");
        MongoCollection<Document> grades = numbersDB.getCollection("students");

        MongoCursor<Document> cursor = grades.find().iterator();

        try {
            while (cursor.hasNext()) {
                Document student = cursor.next();
                int id = student.getInteger("_id");
                Document toBeRemoved = null;
                ArrayList<Document> scores = student.get("scores", ArrayList.class);

                double minHomeworkScore = Double.MAX_VALUE;
                Iterator<Document> it = scores.iterator();

                while (it.hasNext()) {
                    Document elem = it.next();

                    if (elem.getString("type").equals("homework")) {
                        double homeworkScore = elem.getDouble("score");

                        if (homeworkScore < minHomeworkScore) {
                            minHomeworkScore = homeworkScore;
                            toBeRemoved = elem;
                        }
                    }
                }

                grades.updateOne(eq("_id", id), pull("scores", toBeRemoved));
            }
        } finally {
            cursor.close();
        }
    }
}
