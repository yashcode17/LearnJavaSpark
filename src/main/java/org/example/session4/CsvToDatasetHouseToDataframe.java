package org.example.session4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class CsvToDatasetHouseToDataframe {
    //Dataset are useful when we are using user defined types
    //And we can convert dataset to dataframe and vice versa
    public void start(){

        SparkSession session = SparkSession.builder()
                .appName("CSV to Dataframe to DataSet<House> and back")
                .master("local")
                .getOrCreate();

        String fileName = "src/main/java/org/example/session4/resources/houses.csv";

        Dataset<Row> df = session.read().format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .option("sep", ";")
                .load(fileName);

        System.out.println("This is house ingested in dataframe: ");
        df.printSchema();
        df.show();

        // Creating dataset for the pojo
        Dataset<House> houseDS = df.map(new HouseMapper(), Encoders.bean(House.class));

        System.out.println("This is house ingested in dataset: ");
        houseDS.printSchema();
        houseDS.show();

        //Converting dataset to dataframe
        Dataset<Row> df2 = houseDS.toDF();
        df2 = df2.withColumn("formatedDate", concat(df2.col("vacantBy.date"), lit("_"), df2.col("vacantBy.year")));
        df2.show();
    }

//    static void HouseMapper()
}
