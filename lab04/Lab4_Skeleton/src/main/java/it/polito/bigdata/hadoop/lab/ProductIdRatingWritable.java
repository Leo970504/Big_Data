package it.polito.bigdata.hadoop.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProductIdRatingWritable implements org.apache.hadoop.io.Writable {

    private String productId;
    private double rating;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getRating() {
        return rating;
    }

    public ProductIdRatingWritable(String productId, double rating) {
        this.productId = productId;
        this.rating = rating;
    }

    public ProductIdRatingWritable() {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(rating);
        dataOutput.writeUTF(productId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rating = dataInput.readDouble();
        productId = dataInput.readUTF();
    }
}
