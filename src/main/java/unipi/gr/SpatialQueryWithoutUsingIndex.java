package unipi.gr;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;
import org.bson.Document;

import java.util.Arrays;

public class SpatialQueryWithoutUsingIndex {

    public static void main(String args[]){

        long timer = System.currentTimeMillis();

        MongoCredential credential = MongoCredential.createCredential("admin", "test", "abc123".toCharArray());
        MongoClientOptions options = MongoClientOptions.builder()/*.sslEnabled(true)*/.build();
        MongoClient mongoClient = new MongoClient(new ServerAddress("0.0.0.0", 27017), credential, options);
        MongoCollection m = mongoClient.getDatabase("test").getCollection("geoPoints");

        Polygon polygon = new Polygon(Arrays.asList(new Position(21, 35),new Position(25, 35),new Position(25, 38),new Position(21, 38),new Position(21, 35)));
        long d = m.countDocuments(Filters.geoWithin("location", polygon));

        /*
        MongoCursor<Document> cursor = m.find(Filters.geoWithin("location", polygon)).iterator();
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
