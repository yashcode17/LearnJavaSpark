package org.example.session2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {

    public void printSchema(){
        SparkSession sparkSession = SparkSession.builder()
                .appName("Complex CSV to DataFrame")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", true) //header is present
                .option("multiline", true) //data may present in multiple lines
                .option("sep", ";") //separated by ;
                .option("quote", "^") // consider ^ as "
                .option("dateFormat", "M/d/y") //date format
                .option("inferSchema", true) //auto detect the schema(data types)
                .load("src/main/resources/amazonProducts.txt");

        System.out.println("Data Frame content:");
//        df.show(7);
        df.show(7, 90);
        System.out.println("data framer Schema:");
        df.printSchema();
    }
}
