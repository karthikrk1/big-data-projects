package bigram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.TextPair;
/**
 * Created by Karthik Ramakrishnan  on 12/16/17.
 */
public class BigramCounter extends Configured implements Tool{
    public int run(String[] args) throws Exception {
        if(args.length!=2) {
            System.err.println("Wrong Usage");
            System.err.println("Usage: BigramCounter <input path> <output path>");
            return -1;
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Bigram Counter");
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(BigramMapper.class);
        job.setReducerClass(BigramReducer.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BigramCounter(), args);
        System.exit(exitCode);
    }
}
