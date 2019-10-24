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
    
    private int prevPos = 0;
    private int nextPos = 0;

    private String getNextToken(String str) {
        if (nextPos == str.length())
            return null;
        if (nextPos != 0) {
            prevPos = nextPos + 2;
        }
        nextPos = str.indexOf("\\|", prevPos);
        if (nextPos < 0)
            nextPos = str.length();
        return str.substring(prevPos, nextPos);
    }

    private String getNextToken(String str, int skip) {
        if (skip < 0) {
            return null;
        }
        int count = 0;
        String res;
        do {
            res = getNextToken(str);
        }
        while ((count++ < skip) && (res != null));
        return res;
    }

    private void resetParser() {
        prevPos = 0;
        nextPos = 0;
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
                resetParser();
                //String[] separatedLine = line.split("|");
                String name = getNextToken(line);
                String address = getNextToken(line);
                String latitude = getNextToken(line, 2);
                String longitude = getNextToken(line);

                //In Arrays.asList, the first argument should be lot and the second lat
                docs.add( new Document("id", name).append("coordinates",
                                                                      Arrays.asList(Float.parseFloat(longitude), Float.parseFloat(latitude))).append("hotel",address));


            });

            m.insertMany(docs);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


