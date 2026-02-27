package org.example.session4;

public class Main {
    public static void main(String[] args) {
//        ArrayToDataset app = new ArrayToDataset();
//        app.start();

//        CsvToDatasetHouseToDataframe app = new CsvToDatasetHouseToDataframe();
//        app.start();

        WordCount wc = new WordCount();
        wc.start();

        //flatmap -> one input, multiple output (one to many)
        //reduce -> multiple input, one output (many to one)
        //map -> one input, one output (one to one)
    }
}
