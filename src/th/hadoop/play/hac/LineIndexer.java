package th.hadoop.play.hac;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/** 
 * A simple inverted index code adapting from Yahoo  developers tutorial 
 * for Hadoop 0.2x
 */
public class LineIndexer {

    public static class LineIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static Text loc = new Text();
        private final static Text word = new Text();

        @Override
        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();
            loc.set(fileName);
            StringTokenizer itr = new StringTokenizer(val.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, loc);
            }
        }
    }

    public static class LineIndexReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder buf  = new StringBuilder();
            boolean first = true;
            for (Text loc : values) {
                if (!first) {
                    buf.append(", ");                    
                }
                first = false;
                buf.append(loc.toString());
            }
            context.write(key, new Text(buf.toString()));
        }
    }
    
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop jar hac.jar hac.ydn.LineIndexer input out");
            System.exit(2);
        }
        
        Job job = new Job(conf, "LineIndexer");
        job.setJarByClass(LineIndexer.class);
        
        job.setMapperClass(LineIndexMapper.class);
        job.setCombinerClass(LineIndexReducer.class);
        job.setReducerClass(LineIndexReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
        
}
