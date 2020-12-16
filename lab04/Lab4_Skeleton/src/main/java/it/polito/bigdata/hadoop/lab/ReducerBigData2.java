package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                DoubleWritable,   // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type

    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        double totalRating = 0;
        int num = 0;
		/* Implement the reduce method */
    	for (DoubleWritable rating : values) {
            totalRating += rating.get();
            num++;
        }
    	double avg = totalRating / (double) num;

    	context.write(key, new DoubleWritable(avg));
    }
}
