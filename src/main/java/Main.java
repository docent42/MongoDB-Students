import com.mongodb.Block;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BSON;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Filters.*;


public class Main
{
    private static final String COMMA_DELIMITER = ",";
    public static void main(String[] args)
    {

        try (MongoClient mongoClient = MongoClients.create())
        {

            MongoDatabase mongoDB = mongoClient.getDatabase("test");
            MongoCollection<Document> collection = mongoDB.getCollection("students");
            //collection.drop();
            //collection.insertMany(fromCsvToMongo("csv/mongo.csv"));
            System.out.printf("%n1. Students count - %d%n%n",collection.countDocuments());
            int i=0;
            for (Document temp : collection.find(gt("age",40)))
                i++;
            System.out.printf("%n2. Students over 40 years old - %d%n%n",i);
            String youngestStudent = collection.find().sort(new Document("age",1)).limit(1).first().getString("name");
            System.out.printf("%n3. Youngest student on course - %s%n",youngestStudent);

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
                    .append("age",Integer.valueOf(values[1]))
                    .append("courses",Arrays.asList(courses)));
        }
        return records;
    }
}
