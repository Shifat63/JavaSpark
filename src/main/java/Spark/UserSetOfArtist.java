package Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;

public class UserSetOfArtist {
    public void generateUserSet(String inputFile, String appName){
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]");

        JavaSparkContext context = new JavaSparkContext(conf);

        //Read file
        JavaRDD<String> input = context.textFile(inputFile);

        //Extract artist and user
        JavaRDD<String> artistAndUserList = input.flatMap(line-> {
            String[] parts = line.split("\t");
            return Arrays.asList(parts[3]+"\t"+parts[0]).iterator();
        });

        // Make artist user pair
        JavaPairRDD<String, String> artistPairedUser= artistAndUserList.mapToPair(artistAndUser -> {
            String[] parts = artistAndUser.split("\t");
            return new Tuple2<String,String>(parts[0],parts[1]);
        });

        // Aggregate the pairs based on artist
        JavaPairRDD<String, HashSet<String>> artistWithUserList = artistPairedUser.aggregateByKey(
            new HashSet<String>(),
            (a, b) -> {
                a.add(b);
                return a;
            },
            (a, b) -> {
                a.addAll(b);
                return a;
            }
        );

        System.out.println(artistWithUserList.count());
        artistWithUserList.saveAsTextFile("/tmp/UserSetOfArtist.txt");
        context.close();
    }
}
