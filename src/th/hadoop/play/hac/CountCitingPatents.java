package th.hadoop.play.hac;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * Produce a file where each line is a pair of (k, v)
 * k: cited patent number 
 * v: number of citing patents. 
 * i.e. how many patents citing patent k.
 * 
 * Adapting for Hadoop 0.2x from Chuck Lam code in Hadoop in Action book 
 * 
 */
public class CountCitingPatents {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String citation[] = value.toString().split(",");
            context.write(new Text(citation[1]), new Text(citation[0]));
        }
    }
    
    public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text val : values) {
                count ++;
            }            
            String s = String.valueOf(count);            
            context.write(key, new Text(s));
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop jar hac.jar hac.c4.CountCitingPatents in/patents/cite75_99.txt out/count");
            System.exit(2);
        }
        
        Job job = new Job(conf, "CountCitingPatents");
        job.setJarByClass(CountCitingPatents.class);
        
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(ReduceClass.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }    
    
}
