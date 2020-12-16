package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                ProductIdRatingWritable,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type

    protected void reduce(
        Text key, // Input key type
        Iterable<ProductIdRatingWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        ArrayList<String> productIdList = new ArrayList<>();
        ArrayList<Double> ratingsList = new ArrayList<Double>();
        double totalRating = 0;

    	for (ProductIdRatingWritable item : values) {
    	    String productId = item.getProductId();
    	    productIdList.add(productId);
    	    Double rating = item.getRating();
    	    ratingsList.add(rating);
    	    totalRating += rating;
        }

    	double avg = totalRating / (double) ratingsList.size();

    	for (int i = 0; i < productIdList.size(); i++) {
    	    context.write(new Text(productIdList.get(i)), new DoubleWritable(ratingsList.get(i) - avg));
        }
    }
}
