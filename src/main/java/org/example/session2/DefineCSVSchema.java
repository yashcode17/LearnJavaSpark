package org.example.session2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DefineCSVSchema {

    public void printDefinedSchema(){
        SparkSession sparkSession = SparkSession.builder()
                .appName("Complex CSV schema to Data Frame")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "id",
                        DataTypes.IntegerType,
                        false
                ),
                DataTypes.createStructField(
                        "product_id",
                        DataTypes.IntegerType,
                        true
                ),
                DataTypes.createStructField(
                        "item_name",
                        DataTypes.StringType,
                        false
                ),
                DataTypes.createStructField(
                        "published_on",
                        DataTypes.DateType,
                        true
                ),
                DataTypes.createStructField(
                        "url",
                        DataTypes.StringType,
                        false
                )
        });

        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", true)
                .option("multiline", true)
                .option("sep", ";")
                .option("dateFormat", "M/d/y")
                .option("quote", "^")
                .schema(schema)
                .load("src/main/resources/amazonProducts.txt");

        df.show(5, 90);
        df.printSchema();
    }
}
