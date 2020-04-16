import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

    static int i = 0;
    MultipleOutputs multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    @SuppressWarnings("unchecked")
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double num = 0.0;
        double new_centroid_x = 0.0, new_centroid_y = 0.0;
        int data_x = 0, data_y = 0, sum_x = 0, sum_y = 0;
        String data = "";
        String[] strArr;
        if (i == 0)
            data = "Cluster,CustomerID,Gender,Age,AnnualIncome(k$),SpendingScore(1-100)";

        while (values.iterator().hasNext()) {
            strArr = values.iterator().next().toString().split(",");
            data_x = Integer.parseInt(strArr[strArr.length - 2]);
            data_y = Integer.parseInt(strArr[strArr.length - 1]);
            sum_x += data_x;
            sum_y += data_y;
            ++num;
            data = data + "\nCluster" + ((i % 5) + 1);
            for (int j = 0; j < strArr.length; j++) {
                data = data + "," + strArr[j];
            }
        }
        new_centroid_x = sum_x / num;
        new_centroid_y = sum_y / num;
        multipleOutputs.write(new Text(""), new Text(data), "Kmeans_Output");
        multipleOutputs.write(new Text(""),
                new Text(Double.toString(new_centroid_x) + "," + Double.toString(new_centroid_y)), "Centroids");
        i++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}