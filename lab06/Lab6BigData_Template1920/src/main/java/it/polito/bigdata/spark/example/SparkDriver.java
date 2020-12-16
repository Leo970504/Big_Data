package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #6");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> reviewRDD  = sc.textFile(inputPath);
		JavaRDD<String> reviewRDDnoHeader = reviewRDD.filter(line -> !line.startsWith("Id,"));
		JavaPairRDD<String, String> pairUserProduct = reviewRDDnoHeader.mapToPair(s -> {
			String[] features = s.split(",");

			return new Tuple2<String, String>(features[2], features[1]);
		}).distinct();

		JavaPairRDD<String, Iterable<String>> UserIDListOfReviewedProducts = pairUserProduct.groupByKey();

		JavaRDD<Iterable<String>> transactions = UserIDListOfReviewedProducts.values();

		JavaPairRDD<String, Integer> pairsOfProductsOne = transactions.flatMapToPair(products -> {
			List<Tuple2<String, Integer>> result = new ArrayList<>();

			for (String p1 : products) {
				for (String p2 : products) {
					if (p1.compareTo(p2) > 0) {
						result.add(new Tuple2<String, Integer>(p1 + " " + p2, 1));
					}
				}
			}

			return result.iterator();
		});

		JavaPairRDD<String, Integer> pairsFrequencies = pairsOfProductsOne.reduceByKey((count1, count2) -> (count1 + count2));

		JavaPairRDD<String, Integer> pairsFreqOver1 = pairsFrequencies.filter(t -> t._2() > 1);

		JavaPairRDD<Integer, String> freqProducts = pairsFreqOver1.mapToPair(
				freqProduct -> new Tuple2<Integer, String>(freqProduct._2(), freqProduct._1()));

		JavaPairRDD<Integer, String> resultRDD = freqProducts.sortByKey(false);

		resultRDD.saveAsTextFile(outputPath);

		sc.close();
	}
}
