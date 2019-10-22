import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;


public class Main
{
    private static final String COMMA_DELIMITER = ",";
    public static void main(String[] args)
    {

        try (MongoClient mongoClient = MongoClients.create())
        {

            MongoDatabase mongoDB = mongoClient.getDatabase("test");
            MongoCollection<Document> collection = mongoDB.getCollection("students");
            //collection.insertMany(fromCsvToMongo("csv/mongo.csv"));
            //collection.find().first().values().forEach(System.out::println);

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

    }
    private static List<Document> fromCsvToMongo(String path) throws Exception
    {
        List<Document> records = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;
        while ((line = br.readLine()) != null) {
            String[] values = line.split(COMMA_DELIMITER,3);
            String[] courses = values[2].replaceAll("\"","")
                    .split(COMMA_DELIMITER);
            records.add(new Document().append("name",values[0])
                    .append("age",values[1])
                    .append("courses",Arrays.asList(courses)));
        }
        return records;
    }
}
