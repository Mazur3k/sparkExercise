package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		boolean testMode = false;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode); //userId, chapterId
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode); //chapterId, courseId
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode); //courseId, title

		viewData = viewData.distinct().mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterUserCourse = viewData.join(chapterData); //chapter (user, course)
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> userCourse = chapterUserCourse.mapToPair(tuple -> new Tuple2<>(tuple._2, 1)); //(user, course) 1
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> rddWithNumberOfWatchedEpisodes = userCourse.reduceByKey((a, b) -> a + b); //(user, course) number
		JavaPairRDD<Integer, Integer> couseNumberOfWatchedEpisodes = rddWithNumberOfWatchedEpisodes.mapToPair(tuple -> new Tuple2<>(tuple._1._2, tuple._2)); // course number

		JavaPairRDD<Integer, Integer> episodesPerCourse = chapterData.mapToPair(tuple -> new Tuple2<>(tuple._2, 1)).reduceByKey((a, b) -> a + b); //course numberOfEpisodes
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> rddWatchedFromTotal = couseNumberOfWatchedEpisodes.join(episodesPerCourse); //course view of

		JavaPairRDD<Integer, Double> integerDoubleJavaPairRDD = rddWatchedFromTotal.mapToPair(tuple -> new Tuple2<>(tuple._1, (Double.valueOf(tuple._2._1) / Double.valueOf(tuple._2._2)))); // course persentage

		JavaPairRDD<Integer, Long> pointsPerUsersData = integerDoubleJavaPairRDD.mapValues(value -> {
			if (value > 0.9) return 10L;
			if (value > 0.5) return 4L;
			if (value > 0.25) return 2L;
			return 0L;
		});

		JavaPairRDD<Integer, Long> finalResult = pointsPerUsersData.reduceByKey((a, b) -> a + b);
		JavaPairRDD<String, Tuple2<Integer, Long>> rddWithPointsAndTitles = finalResult.join(titlesData).mapToPair(t -> new Tuple2<>(t._2._2, new Tuple2<>(t._1, t._2._1)));
		rddWithPointsAndTitles.foreach(t -> System.out.println(t));

		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
