package wctwo;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Karthik Ramakrishnan on 12/16/17.
 */
public class WCWithTool extends Configured implements Tool{
    public int run(String[] args) throws Exception {
        if(args.length !=2) {
            System.err.println("Wrong Command");
            System.err.println("Usage: <input-data> <output-path> ");
            return -1;
        }
        Job job = Job.getInstance(getConf());
        job.setJobName("Word Count with Tool");
        job.setJarByClass(WCWithTool.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WCWithTool(), args);
        System.exit(exitCode);
    }
}
