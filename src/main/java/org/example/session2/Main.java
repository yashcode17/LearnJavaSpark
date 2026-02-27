package org.example.session2;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Main {

    public static void main(String[] args) {

        //Reducing Logs in the console
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        InferCSVSchema parser1 = new InferCSVSchema();
        parser1.printSchema();

        DefineCSVSchema parser2 = new DefineCSVSchema();
        parser2.printDefinedSchema();

        JSONLineParser parser3 = new JSONLineParser();
        parser3.parseJsonLines();
    }
}
