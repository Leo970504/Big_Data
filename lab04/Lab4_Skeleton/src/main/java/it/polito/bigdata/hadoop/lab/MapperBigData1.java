package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    ProductIdRatingWritable> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
        String[] field = value.toString().split(",");

        if (!field[0].equals("Id")) {
            String productId = field[1];
            String userId  = field[2];
            int rating = Integer.parseInt(field[6]);

            context.write(new Text(userId), new ProductIdRatingWritable(productId, rating));
        }
    }
}
