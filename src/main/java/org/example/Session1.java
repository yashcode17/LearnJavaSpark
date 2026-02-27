package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

//Session to upload data from CSV to database
public class Session1 {
    public static void main(String[] args) {

        //Create a session - to connect with master node
        SparkSession sparkSession = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        //Get data (it is present in resources)
        //usually data come from distributed file system like hadoop or S3
        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", true)
                .load("src/main/resources/name_and_comments.txt");

        df.show();

        //DataSet is immutable structure
        //Transformation

        //Concatenate the values of columns, and create the new column
        df = df.withColumn("full_name", concat(df.col("last_name"), lit(", "), df.col("first_name")));

        //Filter - Get the data having integer in one column
        df = df.filter(df.col("comment").rlike("\\d+"));

        //Order
        df = df.orderBy(df.col("last_name").asc());

        //show first three rows
        df.show();

        //Save data in to database
        String dbConnectionUrl = "jdbc:mysql://localhost:3306/sparkProject";

        Properties properties = new Properties();
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("user", "root");          // ✅ correct key
        properties.setProperty("password", "Yash@1234");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "spark_table", properties);

    }
}
