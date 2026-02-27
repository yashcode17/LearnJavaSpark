package org.example.session4;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ArrayToDataset {
    public void start(){
        SparkSession sparkSession = new SparkSession.Builder()
                .appName("Array to Dataset <String>")
                .master("local")
                .getOrCreate();

        String[] stringList = new String[] {"Banana", "Car", "Glass", "Banana", "Computer", "Car"};

        List<String> data = Arrays.asList(stringList);

        //Creating dataset
        Dataset<String> ds = sparkSession.createDataset(data, Encoders.STRING());

        //Map data and create new dataset
        ds = ds.map(new StringMapper(), Encoders.STRING());
//        ds = ds.map((MapFunction<String, String>) row -> "word: " + row, Encoders.STRING());
        ds.show();

        //Reduce data
        String stringValue = ds.reduce(new StringReducer());
        System.out.println("Reduced String -> " + stringValue);
    }

    static class StringReducer implements ReduceFunction<String>, Serializable{

        @Override
        public String call(String s1, String s2) throws Exception {
            return s1 + s2;
        }
    }

    static class StringMapper implements MapFunction<String, String>, Serializable{

        @Override
        public String call(String s) throws Exception {
            return "word: " + s;
        }
    }
}
