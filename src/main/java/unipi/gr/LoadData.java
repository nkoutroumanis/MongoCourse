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
        LoadData ld = new LoadData();

    }

    private String txtPath;

    public LoadData(String txtPath){
        this.txtPath = txtPath;
    }



    public void loadDataIn2dSphereForm(){

        MongoCredential credential = MongoCredential.createCredential("admin", "test", "abc123".toCharArray());
        MongoClientOptions options = MongoClientOptions.builder()/*.sslEnabled(true)*/.build();
        MongoClient mongoClient = new MongoClient(new ServerAddress("0.0.0.0", 27017), credential, options);
        MongoCollection m = mongoClient.getDatabase("test").getCollection("geoPoints");

        List<Document> docs = new ArrayList<>();

        try{

            Stream<String> lines = Files.lines(Paths.get(txtPath));

            lines.forEach(line -> {

                String[] separatedLine = line.split(";");

                Document embeddedDoc = new Document("type","Point").append("coordinates", Arrays.asList(Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]), Float.parseFloat(separatedLine[numberOfColumnLatitude - 1])));
                docs.add( new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date",dateFormat.parse(separatedLine[numberOfColumnDate - 1])));


            });

            m.insertMany(docs);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadDataIn2dForm(){

        MongoCredential credential = MongoCredential.createCredential("admin", "test", "abc123".toCharArray());
        MongoClientOptions options = MongoClientOptions.builder()/*.sslEnabled(true)*/.build();
        MongoClient mongoClient = new MongoClient(new ServerAddress("0.0.0.0", 27017), credential, options);
        MongoCollection m = mongoClient.getDatabase("test").getCollection("geoPoints");

        List<Document> docs = new ArrayList<>();

        try{

            Stream<String> lines = Files.lines(Paths.get(txtPath));

            lines.forEach(line -> {

                String[] separatedLine = line.split(";");

                docs.add( new Document("objectId", separatedLine[0]).append("coordinates", Arrays.asList(Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]), Float.parseFloat(separatedLine[numberOfColumnLatitude - 1]))).append("date",dateFormat.parse(separatedLine[numberOfColumnDate - 1])));


            });

            m.insertMany(docs);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


