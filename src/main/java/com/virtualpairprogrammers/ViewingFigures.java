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
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = true;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode); //userId, chapterId
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode); //chapterId, courseId
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode); //courseId, title

		// TODO - over to you!

		viewData = viewData.distinct().mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> chapterUserCourse = viewData.join(chapterData); //chapterId - userId - courseId
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> prepareToReduceByKey = chapterUserCourse.mapToPair(tuple -> new Tuple2<>(new Tuple2<>(tuple._2._1, tuple._1), 1));//courseId - chapterId - userId -> (courseId, userId), chapter
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> numberOfWatchedEpisodesByUserAndCourse = prepareToReduceByKey.reduceByKey((a, b) -> a + b); // (courseId, userId), numberOfWatchedEpisodes -> courseId, (userId, numberOfWatchedEpisodes)
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> integerTuple2JavaPairRDD = numberOfWatchedEpisodesByUserAndCourse.mapToPair(tuple -> new Tuple2<>(tuple._1._1, new Tuple2<>(tuple._1._2, tuple._2))); // courseId, (userId, numberOfWatchedEpisodes)

		JavaPairRDD<Integer, Integer> numberOfChapterPerCourse = chapterData.mapToPair(tuple -> new Tuple2<>(tuple._2, 1)).reduceByKey((a, b) -> a + b); //courseId - numberOfCourses
		JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Integer>, Integer>> almostDone = integerTuple2JavaPairRDD.join(numberOfChapterPerCourse); // courseId , (userId, numberOfWatchedEpisodes)-tuple2.1 |||| numberOfCourses- tuple2.2) -> couseId, numberOfWatchedEpisodes/numberOfCourses
		JavaPairRDD<Integer, Double> courseAndPersentage = almostDone.mapToPair(tuple -> new Tuple2<>(tuple._1, (Double.valueOf(tuple._2._1._2) / Double.valueOf(tuple._2._2)) * 100));// couseId, persentage

		integerTuple2JavaPairRDD.foreach(t -> System.out.println(t));
		numberOfChapterPerCourse.foreach(t -> System.out.println(t));
		chapterUserCourse.foreach(t -> System.out.println(t));

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
