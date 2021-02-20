package com.jmb;

import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Joins {

    private static final Logger LOGGER = LoggerFactory.getLogger(Joins.class);
    private static final String SPARK_FILES_FORMAT = "csv";
    private static final String PATH_RESOURCES_DF1 = "src/main/resources/spark-data/employees.csv";
    private static final String PATH_RESOURCES_DF2 = "src/main/resources/spark-data/departments.csv";

    public static void main(String[] args) throws Exception {

        LOGGER.info("Application starting up");
        Joins app = new Joins();
        app.init();
        LOGGER.info("Application gracefully exiting...");
    }

    private void init() throws Exception {
        //Create the Spark Session
        SparkSession session = SparkSession.builder()
                .appName("Joins")
                .master("local").getOrCreate();

        //Ingest data from CSV file into a DataFrame
        Dataset<Row> df = session.read()
                .format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .option("inferSchema", "true")
                .load(PATH_RESOURCES_DF1);

        //Show first 5 records of the Raw ingested DataSet
        df.show(5);

        //Load another file into a different DataFrame.

        //Inspect the second data frame

    }

}
