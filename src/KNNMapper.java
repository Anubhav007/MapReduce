import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KNNMapper extends Mapper<LongWritable, Text, Text, Text> {
    private int income, spending_score;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        income = Integer.parseInt(conf.get("Income"));
        spending_score = Integer.parseInt(conf.get("Spending_score"));
    }

    @Override
    // Computes distance of each data point with the query point and writes it to the reducer
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        int data_x, data_y;
        double dist = 0.0;
        String label;

        String line = value.toString().trim();
        if (line.startsWith("Cluster")) {
            String[] strArr = value.toString().trim().split(",");
            if (!strArr[0].equals("Cluster")) {
                label = strArr[0];
                data_x = Integer.parseInt(strArr[strArr.length - 2].trim());
                data_y = Integer.parseInt(strArr[strArr.length - 1].trim());
                dist = Math.sqrt(Math.pow((data_x - income), 2) + Math.pow((data_y - spending_score), 2));
                context.write(new Text(label), new Text(Double.toString(dist)));
            }
        }
    }
}
