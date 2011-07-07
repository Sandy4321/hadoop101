package th.hadoop.play.hac;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class CitationHistogram {

    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private IntWritable citationCount = new IntWritable();
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String citation[] = value.toString().split("\t");
            citationCount.set(Integer.parseInt(citation[1]));      
            context.write(citationCount, one);
        }
    }
    
    public static class ReduceClass extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {                
                count = count + val.get();
            }   
            context.write(key, new IntWritable(count));
        }
    }
    
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop jar hac.jar hac.c4.CitationHistogram out/count out/hist");
            System.exit(2);
        }
        
        Job job = new Job(conf, "CitationHistogram");
        job.setJarByClass(CitationHistogram.class);
        
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(ReduceClass.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }    
    
}
