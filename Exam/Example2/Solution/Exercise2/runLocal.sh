# Remove folders of the previous run
rm -rf exam_ex2A_out
rm -rf exam_ex2B_out


# Run application
spark-submit  --class it.polito.bigdata.spark.exercise2.SparkDriver --deploy-mode client --master local target/Exam2_Exercise2-1.0.0.jar "exam_ex2_data/PM10Readings.txt" 15.0 exam_ex2A_out exam_ex2B_out


