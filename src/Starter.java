
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Starter extends Configured implements Tool {

    public static final String DELIMITER = ",";
    public static String DATA_FILE = "/Mall_Customers.csv";
    public static String KMEANS_OUTPUT_FILE = "/Kmeans_Output-r-00000";
    public static String KNN_OUTPUT_FILE = "/KNN_Output-r-00000";
    public static String TEST_DATA_FILE_NAME = "/query.txt";
    public static String CENTROID_FILE = "/Centroids-r-00000";
    public static String EXERCISE1_OUTPUT_FOLDER = "/exercise1";
    public static String EXERCISE2_OUTPUT_FOLDER = "/exercise2";

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("<input><output>");
            System.exit(1);
        }
        int code = 1;
        for (int j = 0; j < 5; j++) {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path outputPath = new Path(args[0] + CENTROID_FILE);
            if (fs.exists(outputPath)) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputPath)));
                int i = 1;
                try {
                    String line;
                    line = br.readLine();
                    while (line != null) {
                        System.out.println("line is" + line);
                        conf.set("Centroid" + i, line);
                        i++;
                        line = br.readLine();
                    }
                } finally {
                    br.close();
                }
            } else {
                System.out.println("file not found");
                conf.set("Centroid1", new Random().nextInt(138)+","+new Random().nextInt(100));
                conf.set("Centroid2", new Random().nextInt(138)+","+new Random().nextInt(100));
                conf.set("Centroid3", new Random().nextInt(138)+","+new Random().nextInt(100));
                conf.set("Centroid4", new Random().nextInt(138)+","+new Random().nextInt(100));
                conf.set("Centroid5", new Random().nextInt(138)+","+new Random().nextInt(100));
            }
         // Exercise 1 -  output file Kmeans_Output
            Job job1 = new Job(conf, "KMeans");
            job1.setJarByClass(Starter.class);
            job1.setMapperClass(KMeansMapper.class);
            job1.setReducerClass(KMeansReducer.class);
            FileInputFormat.addInputPath(job1, new Path(args[0] + DATA_FILE));
            FileOutputFormat.setOutputPath(job1, new Path(args[1] + EXERCISE1_OUTPUT_FOLDER));
            job1.setInputFormatClass(TextInputFormat.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            code = job1.waitForCompletion(true) ? 0 : 1;
            fs.rename(new Path(args[1] + EXERCISE1_OUTPUT_FOLDER + CENTROID_FILE), new Path(args[0] + CENTROID_FILE));
            fs.rename(new Path(args[1] + EXERCISE1_OUTPUT_FOLDER + KMEANS_OUTPUT_FILE),
                    new Path(args[0] + KMEANS_OUTPUT_FILE));
            fs.delete(new Path(args[1].toString()), true);
            job1.waitForCompletion(true);
            if (j == 4) {
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(new Path(args[0] + TEST_DATA_FILE_NAME))));
                String line = null;
                int numofColumns = 0;
                while ((line = br.readLine()) != null) {
                    String[] feature = line.split(DELIMITER);
                    numofColumns = feature.length;
                    conf.setInt("Income", Integer.parseInt(feature[numofColumns - 2]));
                    conf.setInt("Spending_score", Integer.parseInt(feature[numofColumns - 1]));
                }
                br.close();
                fs.close();
                //Exercise 2 - Labelling clusters
                // Calculating the median of income and spending score centroids and classifying the cluster centroids accordingly
                List<Double> incomeList = new ArrayList<Double>();
                List<Double> spendingscoreList = new ArrayList<Double>();
                Double medianIncome, medianSpendingScore;
                for(int r = 1;r<=5;r++) {
                    incomeList.add(Double.parseDouble(conf.get("Centroid" + r).split(",")[0].trim()));
                    spendingscoreList.add(Double.parseDouble(conf.get("Centroid" + r).split(",")[1].trim()));
                }
                Collections.sort(incomeList);
                Collections.sort(spendingscoreList);
                medianIncome = incomeList.get((incomeList.size()+1)/2 - 1);
                medianSpendingScore = spendingscoreList.get((spendingscoreList.size()+1)/2 - 1);
                String income = "", spending = "";
                for(int r=0;r<5;r++) {

                     if(Math.abs(incomeList.get(r)-medianIncome)/medianIncome <= 0.5)
                        income = "Medium Income";
                    else if(incomeList.get(r)>medianIncome)
                        income = "High Income";
                    else if(incomeList.get(r)<medianIncome) 
                         income = "Low Income";
                    if(Math.abs(spendingscoreList.get(r)-medianSpendingScore)/medianSpendingScore <= 0.5)
                         spending = "Medium Spending Score";
                    else if(spendingscoreList.get(r)>medianSpendingScore)
                        spending = "High Spending Score";
                    else if(spendingscoreList.get(r)<medianSpendingScore) 
                        spending = "Low Spending Score";

                    System.out.println("Cluster"+(r+1)+" corresponds to "+income+" "+spending);
                }
                
                // Exercise 3 -  output file KNN_Output
                Job job = new Job(conf, "KNN");
                FileInputFormat.addInputPath(job, new Path(args[0] + KMEANS_OUTPUT_FILE));
                FileOutputFormat.setOutputPath(job, new Path(args[1] + EXERCISE2_OUTPUT_FOLDER));
                job.setJarByClass(Starter.class);
                job.setMapperClass(KNNMapper.class);
                job.setReducerClass(KNNReducer.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                code = job.waitForCompletion(true) ? 0 : 1;
                fs.rename(new Path(args[1] + EXERCISE2_OUTPUT_FOLDER + KNN_OUTPUT_FILE),
                        new Path(args[0] + KNN_OUTPUT_FILE));
                fs.delete(new Path(args[1].toString()), true);
            }
        }

        return code;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.exit(ToolRunner.run(conf, new Starter(), args));
    }
}