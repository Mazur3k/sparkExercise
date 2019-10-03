package com.virtualpairprogrammers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class SparkSqlExample {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession.builder().appName("spark").master("local[*]").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataset.createOrReplaceTempView("Students");

        Dataset<Row> averageScorePerSubject = spark.sql("select subject, year, avg(score) as avg_score from Students group by subject, year order by year");
        averageScorePerSubject.show(100);


    }
}
