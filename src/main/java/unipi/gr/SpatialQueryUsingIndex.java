package unipi.gr;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;

public class SpatialQueryUsingIndex {

    public static void main(String args[]){

        long timer = System.currentTimeMillis();

        MongoCredential credential = MongoCredential.createCredential("admin", "test", "abc123".toCharArray());
        MongoClientOptions options = MongoClientOptions.builder()/*.sslEnabled(true)*/.build();
        MongoClient mongoClient = new MongoClient(new ServerAddress("0.0.0.0", 27017), credential, options);
        MongoCollection m = mongoClient.getDatabase("test").getCollection("geoPoints");

        long d = m.countDocuments((and(gte("location.coordinates.0",21),gte("location.coordinates.1",35),lte("location.coordinates.0",25),lte("location.coordinates.1",38))));


        /*
        MongoCursor<Document> cursor = m.find((and(gte("location.coordinates.0",21),gte("location.coordinates.1",35),lte("location.coordinates.0",25),lte("location.coordinates.1",38)))).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
        */

        System.out.println("Execution Time (ms): "+ (System.currentTimeMillis()-timer));

    }

}
