package wctwo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Created by Karthik Ramakrishnan on 12/16/17.
 */
public class WCWithCombiner {
    public static void main(String[] args) throws Exception {
        if(args.length!=2) {
            System.err.println("Wrong Usage");
            System.err.println("Usage: wctwo.WCWithCombiner <input-data> <output-path>");
            System.exit(0);
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Word Count v2 with Combiner");
        job.setJarByClass(WCWithCombiner.class);
        job.setJobName("Word Count with Combiner");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setCombinerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
