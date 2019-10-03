package com.virtualpairprogrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class KeyWordsExample {

    private static SparkConf conf = new SparkConf().setAppName("SparkExcercise").setMaster("local[*]");
    private static JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) {
        JavaRDD<String> keyWords = loadData()
                .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .flatMap((sentence) -> Arrays.asList(sentence.split(" ")).iterator())
                .filter((word) -> !Util.isBoring(word) && word.trim().length()>0);
        JavaPairRDD<String, Integer> numberOfEachWords = keyWords.mapToPair((word) -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
        JavaPairRDD<Integer, String> preparedToSortByKey = numberOfEachWords.mapToPair((tuple) -> new Tuple2<>(tuple._2, tuple._1));
        List<Tuple2<Integer, String>> topTen = preparedToSortByKey.sortByKey(false).take(10);
        topTen.stream().forEach(System.out::println);
    }

    public static JavaRDD<String> loadData(){
        return sc.textFile("src/main/resources/subtitles/input.txt");
    }

}
