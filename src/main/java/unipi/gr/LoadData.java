package unipi.gr;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class LoadData {

    public static void main(String args[]){
        LoadData ld = new LoadData("./hotels/hotels.txt");
        ld.loadDataIn2dSphereForm();

        //db.collection.createIndex( { location : "2dsphere" } )
        //db.collection.createIndex( { coordinates : "2d" } )

    }

    private String txtPath;

    public LoadData(String txtPath){
        this.txtPath = txtPath;
    }



    public void loadDataIn2dSphereForm(){

        MongoCredential credential = MongoCredential.createCredential("admin", "test", "abc123".toCharArray());
        MongoClientOptions options = MongoClientOptions.builder().build();
        MongoClient mongoClient = new MongoClient(new ServerAddress("0.0.0.0", 27017), credential, options);
        MongoCollection m = mongoClient.getDatabase("test").getCollection("geoPoints");

        List<Document> docs = new ArrayList<>();

        try{

            Stream<String> lines = Files.lines(Paths.get(txtPath));

            lines.forEach(line -> {

                String[] separatedLine = line.split("|");

                //In Arrays.asList, the first argument should be lot and the second lat
                Document embeddedDoc = new Document("type","Point").append("coordinates", Arrays.asList(Float.parseFloat(separatedLine[5]), Float.parseFloat(separatedLine[4])));
                docs.add( new Document("id", separatedLine[0]).append("location", embeddedDoc).append("hotel",separatedLine[1]));


            });

            m.insertMany(docs);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadDataIn2dForm(){

        MongoCredential credential = MongoCredential.createCredential("admin", "test", "abc123".toCharArray());
        MongoClientOptions options = MongoClientOptions.builder().build();
        MongoClient mongoClient = new MongoClient(new ServerAddress("0.0.0.0", 27017), credential, options);
        MongoCollection m = mongoClient.getDatabase("test").getCollection("points");

        List<Document> docs = new ArrayList<>();

        try{

            Stream<String> lines = Files.lines(Paths.get(txtPath));

            lines.forEach(line -> {

                String[] separatedLine = line.split("|");

                //In Arrays.asList, the first argument should be lot and the second lat
                docs.add( new Document("id", separatedLine[0]).append("coordinates", Arrays.asList(Float.parseFloat(separatedLine[5]), Float.parseFloat(separatedLine[4]))).append("hotel",separatedLine[1]));


            });

            m.insertMany(docs);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


