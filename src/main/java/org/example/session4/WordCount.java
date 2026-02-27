package org.example.session4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WordCount {

    public void start(){
        String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" +
                "'for', 'if', 'in', 'into', 'is', 'it',\r\n" +
                "'no', 'not', 'of', 'on', 'or', 'such',\r\n" +
                "'that', 'the', 'their', 'then', 'there', 'these',\r\n" +
                "'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," +
                "'your', 'you', 'I', "
                + " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";

        SparkSession session = SparkSession.builder()
                .appName("Unstructured text to flatmap")
                .master("local")
                .getOrCreate();

        String filePath = "src/main/java/org/example/session4/resources/shakespeare.txt";

        Dataset<Row> df = session.read().format("text")
                .load(filePath);

        df.show(10);

        //flatmap -> one to many

        Dataset<String> wordsDS = df.flatMap(new LineMapper(), Encoders.STRING());
//        wordsDS.show();

        Dataset<Row> df2 = wordsDS.toDF();

        df2 = df2.groupBy("value").count();
        df2 = df2.orderBy(df2.col("count").desc());
        df2 = df2.filter("lower(value) NOT IN " + boringWords);
        df2.show(500);
    }
}
