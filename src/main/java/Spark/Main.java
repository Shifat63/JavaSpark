package Spark;

import java.util.HashSet;
import java.util.Set;

public class Main {
    public static void main(String[] args) {
        String inputFile= "resources/last-fm-sample100000.tsv";
        String appName = "CountListeningEventPerArtist";

//        Task 1
        UserClicks userClicks = new UserClicks();
        userClicks.listeningEventCounter(inputFile, appName);

//        Task 2
        UserSetOfArtist userSetOfArtist = new UserSetOfArtist();
        userSetOfArtist.generateUserSet(inputFile, appName);

//        Task 3
        Set<String> userSet = new HashSet<String>();
        userSet.add("user_000715");
        userSet.add("user_000774");
        userSet.add("user_000841");
        System.out.println("Set during initialization: " + userSet);

        //Initialize an object of UserSet class
        UserSet userSetObject = new UserSet(userSet);
        // Adding a new user to the userSet
        System.out.println("Set after adding a new user: " + userSetObject.add("user_000896"));

        // Initializing a new userSet
        Set<String> secondUserSet = new HashSet<String>();
        secondUserSet.add("user_000715");
        secondUserSet.add("user_000544");
        System.out.println("Second User set: " + secondUserSet);

        // Adding a new userSet to the old userSet
        System.out.println("Set after adding second User set: " + userSetObject.add(secondUserSet));

        secondUserSet.add("user_000127");
        System.out.println("Set we want to check distance from: " + secondUserSet);
        System.out.println("Distance: "+ userSetObject.distanceTo(secondUserSet));

    }
}
