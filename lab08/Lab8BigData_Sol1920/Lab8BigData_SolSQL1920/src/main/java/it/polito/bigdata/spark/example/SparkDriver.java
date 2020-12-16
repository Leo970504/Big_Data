package it.polito.bigdata.spark.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.sql.Timestamp;

import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];

		// Create a Spark Session object and set the name of the application
		SparkSession ss = SparkSession.builder().appName("Spark Lab #8 - Template").getOrCreate();

		// Read the content of the input file register.csv and store it into a
		// DataFrame
		// The input file has an header
		// Schema of the input data:
		// |-- station: integer (nullable = true)
		// |-- timestamp: timestamp (nullable = true)
		// |-- used_slots: integer (nullable = true)
		// |-- free_slots: integer (nullable = true)
		Dataset<Row> inputDF = ss.read().format("csv").option("delimiter", "\\t").option("header", true)
				.option("inferSchema", true).load(inputPath);

		// Cast the DataFrame to a typed Dataset<Readings>
		Dataset<Reading> inputDS = inputDF.as(Encoders.bean(Reading.class));

		// Assign the “table name” readings to the inputDS Dataset
		inputDS.createOrReplaceTempView("readings");

		// Define a User Defined Function called full(Integer free_slots)
		// that returns 1 if the value of free_slots is equal to 0,
		// 1 if free_slots is greater than 0.
		ss.udf().register("full", (Integer free_slots) -> {
			if (free_slots == 0)
				return 1;
			else
				return 0;
		}, DataTypes.IntegerType);

		// Define a User Defined Function that returns the DayOfTheWeek
		// given a Timestamp value
		ss.udf().register("day_of_week", (Timestamp date) -> DateTool.DayOfTheWeek(date), DataTypes.StringType);

		// Select only the lines with free_slots<>0 or used_slots<>0
		// and then compute the criticality for each group (station, dayofweek,
		// hour) (i.e., for each pair (station, timeslot))
		// and finally select only the groups with criticality>threshold.
		//
		// The criticality is equal to the average of full(free_slots)
		// Store the result in a typed Dataset<StationDayHourCriticality>
		// The schema is:
		// |-- station: integer (nullable = true)
		// |-- dayofweek: string (nullable = true)
		// |-- hour: integer (nullable = true)
		// |-- criticality: double (nullable = true)
		Dataset<StationDayHourCriticality> selectedPairsDS = ss
				.sql("SELECT station, day_of_week(timestamp) as dayofweek, hour(timestamp) as hour,  "
						+ "avg(full(free_slots)) as criticality " + "FROM readings "
						+ "WHERE free_slots<>0 OR used_slots<>0 "
						+ "GROUP BY station, day_of_week(timestamp), hour(timestamp) " + "HAVING avg(full(free_slots))>"
						+ threshold)
				.as(Encoders.bean(StationDayHourCriticality.class));

		// Assign the “table name” criticals to the selectedPairsDS Dataset
		selectedPairsDS.createOrReplaceTempView("criticals");

		// Read the content of the input file stations.csv and store it into a
		// DataFrame
		// The input file has an header
		// Schema of the input data:
		// |-- id: integer (nullable = true)
		// |-- longitude: double (nullable = true)
		// |-- latitude: double (nullable = true)
		// |-- name: string (nullable = true)
		Dataset<Row> stationsDF = ss.read().format("csv").option("delimiter", "\\t").option("header", true)
				.option("inferSchema", true).load(inputPath2);

		// Cast the DataFrame to a typed Dataset<Readings>
		Dataset<Station> stationsDS = stationsDF.as(Encoders.bean(Station.class));

		// Assign the “table name” stations to the stationsDS Dataset
		stationsDS.createOrReplaceTempView("stations");

		// Join the selected critical "situations" with the stations table to
		// retrieve the coordinates of the stations.
		// Select only the column station, longitude, latitude and criticality
		// and sort records by criticality (desc), station (asc)
		// Cast the result to a typed Dataset<FinalRecords>
		// Schema
		Dataset<FinalRecord> selectedPairsIdCoordinatesCriticalityDS = ss
				.sql("SELECT station, dayofweek, hour, longitude, latitude, criticality FROM criticals, stations "
						+ "WHERE criticals.station = stations.id "
						+ "ORDER BY criticality DESC, station, dayofweek, hour")
				.as(Encoders.bean(FinalRecord.class));
		
		selectedPairsIdCoordinatesCriticalityDS.show();

		// Save the result in the output folder
		selectedPairsIdCoordinatesCriticalityDS.write().format("csv").option("header", true)
				.save(outputFolder);

		// Close the Spark session
		ss.stop();
	}
}
