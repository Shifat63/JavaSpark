package Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class UserClicks {
    public void listeningEventCounter(String inputFile, String appName){
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]");

        JavaSparkContext context = new JavaSparkContext(conf);

        //Read file
        JavaRDD<String> input = context.textFile(inputFile);

        //Split lines and extract artist name
        JavaRDD<String> artists = input.flatMap(line-> {
            String[] parts = line.split("\t");
            return Arrays.asList(parts[3]).iterator();
        });

        JavaPairRDD<String, Integer> artistListened = artists.mapToPair(artist -> {
            return new Tuple2<String,Integer>(artist,new Integer(1));
        });

        JavaPairRDD<String, Integer> artistListeningEvents = artistListened.reduceByKey((a,b) ->  a + b);

        System.out.println(artistListeningEvents.count());
        artistListeningEvents.saveAsTextFile("/tmp/ListeningEventsPerArtist.txt");
        context.close();
    }
}
