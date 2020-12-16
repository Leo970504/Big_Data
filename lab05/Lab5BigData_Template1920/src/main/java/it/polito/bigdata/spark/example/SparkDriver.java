package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		String outputPath;
		String prefix;
		
		inputPath=args[0];
		outputPath=args[1];
		prefix=args[2];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #5");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

		/** Task 1
		*/
		JavaRDD<String> filteredWordRDD = wordFreqRDD.filter(logline -> logline.toLowerCase().startsWith(prefix));
		System.out.println("Filter 1 - Number of select lines: " + filteredWordRDD.count());

		JavaRDD<Integer> frequenciesRDD = filteredWordRDD.map(line -> Integer.parseInt(line.split("\t")[1])).cache();
		Integer maxFreq = frequenciesRDD.reduce((v1, v2) -> Integer.max(v1, v2));
		System.out.println("Max frequency: " + maxFreq);

		/** Task 2
		 */
		JavaRDD<String> filteredWordsFreqRDD = filteredWordRDD.filter(
				line -> Double.compare(Double.parseDouble(line.split("\t")[1]), 0.8 * maxFreq) > 0);

		System.out.println("Number of filtered pairs: " + filteredWordsFreqRDD.count());

		filteredWordsFreqRDD.map(line -> line.split("\t")[0]).saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
