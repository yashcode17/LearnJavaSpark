package org.example.session2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLineParser {

    public void parseJsonLines(){
        SparkSession sparkSession = SparkSession.builder()
                .appName("JSON lines to DataFrame")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().format("json")
                .load("src/main/resources/simple.json");

        Dataset<Row> df2 = sparkSession.read().format("json")
                .option("multiline", true)
                .load("src/main/resources/multiline.json");

        df.show(5, 90);
        df.printSchema();
        df2.show(5);
        df2.printSchema();
    }
}
