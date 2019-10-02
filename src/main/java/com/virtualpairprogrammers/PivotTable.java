package com.virtualpairprogrammers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.io.File;

public class PivotTable {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession.builder().appName("spark").master("local[*]").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

        dataset = dataset.select(col("year"), col("subject"), col("score").cast(DataTypes.DoubleType).alias("scores"));
        dataset = dataset.groupBy(col("year")).pivot("subject").avg("scores").orderBy(col("year"));
        dataset.show();
    }
}
