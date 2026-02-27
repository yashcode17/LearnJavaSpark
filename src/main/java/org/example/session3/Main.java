package org.example.session3;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Combined 2 Datasets")
                .master("local")
                .getOrCreate();

        //Extract the data and transform
        Dataset<Row> durhamDf = buildDuhramParksDataFrame(sparkSession);
        durhamDf.show(5);

        Dataset<Row> philDf = buildPhilParksDataFrame(sparkSession);
        philDf.show(5);

        combineDataFrames(philDf, durhamDf);

    }

    private static void combineDataFrames(Dataset<Row> df1, Dataset<Row> df2) {

        //Match by column names using the unionByName() method
        Dataset<Row> df = df1.unionByName(df2);
        df.printSchema();
        df.show(500);
        System.out.println("We have " + df.count() + " records");

        Partition[] partitions = df.rdd().partitions();
        System.out.println("Total number of Partitions: " + partitions.length);
    }

    public static Dataset<Row> buildDuhramParksDataFrame(SparkSession session){
        Dataset<Row> df = session.read().format("json")
                .option("multiline", true)
                .load("src/main/java/org/example/session3/resources/durham-parks.json");

        df = df.withColumn("park_id",
                concat(df.col("datasetid"),
                        lit("_"),
                        df.col("fields.objectid"),
                        lit("_Durhum")))
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("city", lit("Durhum"))
                .withColumn("address", df.col("fields.address"))
                .withColumn("has_playground", df.col("fields.playground"))
                .withColumn("zipcode", df.col("fields.zip"))
                .withColumn("land_in_acres", df.col("fields.acres"))
                .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
                .withColumn("geoY", df.col("geometry.coordinates").getItem(1))
                .drop("fields").drop("geometry").drop("record_timestamp").drop("recordid").drop("datasetid");


        return df;
    }

    public static Dataset<Row> buildPhilParksDataFrame(SparkSession session){
        Dataset<Row> df = session.read().format("csv")
                .option("multiline", true)
                .option("header", true)
                .load("src/main/java/org/example/session3/resources/philadelphia_recreations.csv");

//        df = df.filter(lower(df.col("USE_").like("%park%")));
        df = df.filter(" lower(USE_) like '%park%' "); //Using SQL annotation

        df = df.withColumn("park_id", concat(lit("phil_"), df.col("OBJECTID")))
                .withColumnRenamed("ASSET_NAME", "park_name")
                .withColumn("city", lit("Philadelphia"))
                .withColumnRenamed("ADDRESS", "address")
                .withColumn("has_playground", lit("UNKNOWN"))
                .withColumnRenamed("ZIPCODE", "zipcode")
                .withColumnRenamed("ACREAGE", "land_in_acres")
                .withColumn("geoX", lit("UNKNOWN"))
                .withColumn("geoY", lit("UNKNOWN"))
                .drop("OBJECTID").drop("SITE_NAME").drop("CHILD_OF")
                .drop("TYPE").drop("USE_").drop("DESCRIPTION")
                .drop("SQ_FEET").drop("ALLIAS").drop("CHRONOLOGY").drop("NOTES")
                .drop("DATE_EDITED").drop("EDITED_BY").drop("OCCUPANT").drop("TENANT")
                .drop("LABEL");

        return df;
    }

}
