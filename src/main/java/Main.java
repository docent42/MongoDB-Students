import com.mongodb.Block;
import com.mongodb.client.*;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.gt;


public class Main
{
    private static final String COMMA_DELIMITER = ",";
    public static void main(String[] args)
    {

        try (MongoClient mongoClient = MongoClients.create())
        {
            // -------------------- Connection to base --------------------------------------

            MongoDatabase mongoDB = mongoClient.getDatabase("test");
            MongoCollection<Document> collection = mongoDB.getCollection("students");

            // <- расскоментить по надобности сброса ->
            //collection.drop();

            //<- расскоментить по надобности залива информации ->
            //collection.insertMany(fromCsvToMongo("csv/mongo.csv"));

            // ---------------------- Tasks ------------------------------------------------
            System.out.printf("%n1. Students count - %d%n%n",collection.countDocuments());
            int i=0;
            //------------------------------------------------------------------------------

            for (Document temp : collection.find(gt("age",40)))
                i++;

            System.out.printf("%n2. Students over 40 years old - %d%n%n",i);
            //-------------------------------------------------------------------------------

            int youngestAge = (Integer) collection.find()
                    .sort(new Document("age",1))
                    .limit(1).first()
                    .get("age");
            FindIterable<Document> youngestList = collection
                                                .find(new Document().append("age",youngestAge));

            System.out.println("3. Youngest student on course:\n");
            for (Document doc : youngestList)
                System.out.println("- " + doc.getString("name"));

            //-------------------------------------------------------------------------------

            int oldestAge = (Integer) collection.find()
                    .sort(new Document("age",-1))
                    .limit(1).first()
                    .get("age");
            FindIterable<Document> oldestList = collection
                                                .find(new Document().append("age",oldestAge));

            System.out.println("\n4. The courses of oldest students are :\n");
            for (Document doc : oldestList) {
                System.out.print("- " + doc.getString("name") + ": ");
                doc.values().stream().skip(3).forEach(System.out::println);
            }
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
