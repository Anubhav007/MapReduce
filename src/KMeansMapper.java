
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
    List<List<Double>> centroidList = new ArrayList<List<Double>>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        for (int i = 1; i <= 5; i++) {
            List<Double> column = new ArrayList<Double>();
            column.add(Double.parseDouble(context.getConfiguration().get("Centroid" + i).split(",")[0].trim()));
            column.add(Double.parseDouble(context.getConfiguration().get("Centroid" + i).split(",")[1].trim()));
            centroidList.add(column);
        }
    }

    @Override
    // Calculates distance of each data point from the centroids
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        double min_dist = Integer.MAX_VALUE;
        List<Double> centroid = new ArrayList<Double>();
        double dist = 0.0;
        int data_x, data_y;
        String[] strArr = value.toString().split(",");
        if (!strArr[0].equals("CustomerID")) {
            data_x = Integer.parseInt(strArr[strArr.length - 2].trim());
            data_y = Integer.parseInt(strArr[strArr.length - 1].trim());
            for (int i = 0; i < centroidList.size(); i++) {
                dist = Math.sqrt(Math.pow((data_x - centroidList.get(i).get(0)), 2)
                        + Math.pow((data_y - centroidList.get(i).get(1)), 2));
                if (dist <= min_dist) {
                    min_dist = dist;
                    centroid = centroidList.get(i);
                }
            }
            context.write(new Text(Double.toString(centroid.get(0)) + "," + Double.toString(centroid.get(1))),
                    new Text(value));
        }
    }
}