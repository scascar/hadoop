import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Transpose {


    public static class MyMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] numbers = value.toString().split(",");
            for(int i = 0;i<numbers.length;i++){
                context.write( new Text(Integer.toString(i)),new Text(key.toString()+","+numbers[i]));
            }
        }
    }

    public static class MyReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<Integer,String> valuesMap = new HashMap<Integer, String>();
            String resultString = "";
            for (Text val : values) {
                if(val.toString().contains(","))
                    valuesMap.put(Integer.parseInt(val.toString().split(",")[0]),val.toString().split(",")[1]);
            }
            SortedSet<Integer> keys = new TreeSet<Integer>(valuesMap.keySet());
            for (Integer keykey : keys){
                resultString += valuesMap.get(keykey) + ",";
            }
            result.set(resultString.substring(0,resultString.length()-1));
            System.out.println("SORTIE "+ result.toString());
            context.write(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Transpose Matrix");
        job.setJarByClass(Transpose.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
