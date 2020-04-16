import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class KNNReducer extends Reducer<Text, Text, Text, Text> {
    static List<Double> arr = new ArrayList<Double>();
    private MultipleOutputs multipleOutputs;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    @SuppressWarnings("unchecked")
    //Computes the top 3 similiar data points to the query point
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // create a hash map of cluster no and its top 3 matches with the query point
        HashMap<String, List<Double>> LabelDistance = new HashMap<String, List<Double>>();
        List<Double> row = new ArrayList<Double>();
        while (values.iterator().hasNext()) {
            row.add(Double.parseDouble(values.iterator().next().toString()));
        }
        //sorting the list for a cluster based on the distance from query point
        Collections.sort(row);
        LabelDistance.put(key.toString(), row.subList(0, 3));

        for (Map.Entry<String, List<Double>> i : LabelDistance.entrySet()) {
            arr.addAll(i.getValue());
        }

        List<Double> sortedList = new ArrayList<Double>(arr);
        Collections.sort(sortedList);
        // Retrieving the index of Cluster from the sorted list
        int i1 = arr.indexOf(sortedList.get(0)) / 3 + 1;
        int i2 = arr.indexOf(sortedList.get(1)) / 3 + 1;
        int i3 = arr.indexOf(sortedList.get(2)) / 3 + 1;
        List<Integer> list = Arrays.asList(i1, i2, i3);
        // Getting the majority vote from the 3 nearest neighbours
        int max = 0;
        int curr = 0;
        int currKey = 0;
        Set<Integer> unique = new HashSet<Integer>(list);
        for (int key1 : unique) {
            curr = Collections.frequency(list, key1);
            if (max < curr) {
                max = curr;
                currKey = key1;
            }
        }
        // Writing at the end of the last reducer
        if (arr.size() == 15)
            multipleOutputs.write(new Text("Cluster" + currKey), new Text(""), "KNN_Output");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

}
