package it.polito.bigdata.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #7");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		JavaRDD<String> filteredRDD = inputRDD.filter(line -> {
			if (line.startsWith("s")) {
				return false;
			} else {
				String[] fields = line.split("\\t");
				int usedSlots = Integer.parseInt(fields[2]);
				int freeSlots = Integer.parseInt(fields[3]);

				if (usedSlots != 0 || freeSlots != 0) {
					return true;
				} else {
					return false;
				}
			}
		});

		JavaPairRDD<String, CountTotReadingsTotFull> stationWeekDayHour = filteredRDD.mapToPair(line -> {
			// station timestamp used free
			// 1 2008-05-15 12:01:00 0 18

			String[] fields = line.split("\\t");
			int freeSlots = Integer.parseInt(fields[3]);

			String[] timestamp = fields[1].split(" ");
			String dayOfWeek = DateTool.DayOfTheWeek(timestamp[0]);
			String hour = timestamp[1].replaceAll(":.*", "");

			CountTotReadingsTotFull countRF;

			if (freeSlots == 0) {
				countRF = new CountTotReadingsTotFull(1, 1);
			} else {
				countRF = new CountTotReadingsTotFull(1, 0);
			}
			return new Tuple2<String, CountTotReadingsTotFull>(fields[0] + "_" + dayOfWeek + "_" + hour, countRF);
		});

		JavaPairRDD<String, CountTotReadingsTotFull> stationWeekDayHourCounts = stationWeekDayHour.reduceByKey(
				(element1, element2) -> new CountTotReadingsTotFull(element1.numReadings + element2.numReadings,
						element1.numFullReadings + element2.numFullReadings)
		);

		JavaPairRDD<String, Double> stationWeekDayHourCriticality = stationWeekDayHourCounts.mapValues(
				value -> (double) value.numFullReadings / (double) value.numReadings
		);

		JavaPairRDD<String, Double> selectedPairs = stationWeekDayHourCriticality.filter(
				value -> {
					double criticality = value._2.doubleValue();

					if (criticality >= threshold) {
						return true;
					} else {
						return false;
					}
				}
		);

		JavaPairRDD<String, DayOfWeekHourCrit> stationTimeslotCrit = selectedPairs.mapToPair(
				item -> {
					String[] field = item._1().split("_");
					String stationId = field[0];
					String dayOfWeek = field[1];
					Integer hour = Integer.parseInt(field[2]);

					Double criticality = item._2();

					return new Tuple2<>(stationId,
							new DayOfWeekHourCrit(dayOfWeek, hour, criticality));
				}
		);

		JavaPairRDD<String, DayOfWeekHourCrit> resultRDD = stationTimeslotCrit.reduceByKey(
				(DayOfWeekHourCrit value1, DayOfWeekHourCrit value2) -> {
					if (value1.criticality > value2.criticality
					|| (value1.criticality == value2.criticality && value1.hour < value2.hour)
					|| (value1.criticality == value2.criticality && value1.hour == value2.hour
					&& value1.dayOfTheWeek.compareTo(value2.dayOfTheWeek) < 0)) {
						return new DayOfWeekHourCrit(value1.dayOfTheWeek, value1.hour, value1.criticality);
					} else {
						return new DayOfWeekHourCrit(value2.dayOfTheWeek, value2.hour, value2.criticality);
					}
				}
		);
		// Store in resultKML one String, representing a KML marker, for each station 
		// with a critical timeslot 
		// JavaRDD<String> resultKML = .....
		JavaRDD<String> temp = sc.textFile(inputPath2).filter(line -> {
			if (line.startsWith("id")) {
				return false;
			} else {
				return true;
			}
		});

		JavaPairRDD<String, String> stationLocation = sc.textFile(inputPath2).mapToPair(
				line -> {
					String[] fields = line.split("\\t");

					return new Tuple2<>(fields[0], fields[1] + "," + fields[2]);
				}
		);

		JavaPairRDD<String, Tuple2<DayOfWeekHourCrit, String>> resultLocations = resultRDD.join(stationLocation);

		JavaRDD<String> resultKML = resultLocations.map((Tuple2<String, Tuple2<DayOfWeekHourCrit, String>> item) -> {

			String stationId = item._1();
			Tuple2<DayOfWeekHourCrit, String> pair = item._2();

			DayOfWeekHourCrit dayOfWeekHourCrit = pair._1();
			String location = pair._2();
			String result = "<Placemark><name>" + stationId + "</name>" + "<ExtendedData>"
					+ "<Data name=\"DayWeek\"><value>" + dayOfWeekHourCrit.dayOfTheWeek + "</value></Data>"
					+ "<Data name=\"Hour\"><value>" + dayOfWeekHourCrit.hour + "</value></Data>"
					+ "<Data name=\"Criticality\"><value>" + dayOfWeekHourCrit.criticality + "</value></Data>"
					+ "</ExtendedData>" + "<Point>" + "<coordinates>" + location + "</coordinates>"
					+ "</Point>" + "</Placemark>";

			return result;
		});

		// Invoke coalesce(1) to store all data inside one single partition/i.e., in one single output part file
		// resultKML.coalesce(1).saveAsTextFile(outputFolder);
		resultKML.coalesce(1).saveAsTextFile(outputFolder);

		// Close the Spark context
		sc.close();
	}
}
