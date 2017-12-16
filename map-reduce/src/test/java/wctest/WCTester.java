package wctest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import wctwo.WCMapper;
import wctwo.WCReducer;
import wctwo.MinReducer;

/**
 * Created by Karthik Ramakrishnan on 12/16/17.
 */


public class WCTester {
    // Declare some class variables for testing
    MapDriver<LongWritable, Text, Text, IntWritable> mDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> rDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mrDriver;

    @Before
    public void setUp() {
        WCMapper wcMapper = new WCMapper();
        mDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mDriver.setMapper(wcMapper);

        WCReducer wcReducer = new WCReducer();
        rDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        rDriver.setReducer(wcReducer);

        mrDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
        mrDriver.setMapper(wcMapper);
        mrDriver.setReducer(wcReducer);
    }

    @Test
    public void testMapper() throws IOException {
        mDriver.withInput(new LongWritable(1), new Text("I read the book about the history of India"));
        mDriver.withOutput(new Text("I"), new IntWritable(1));
        mDriver.withOutput(new Text("read"), new IntWritable(1));
        mDriver.withOutput(new Text("the"), new IntWritable(1));
        mDriver.withOutput(new Text("book"), new IntWritable(1));
        mDriver.withOutput(new Text("about"), new IntWritable(1));
        mDriver.withOutput(new Text("the"), new IntWritable(1));
        mDriver.withOutput(new Text("history"), new IntWritable(1));
        mDriver.withOutput(new Text("of"), new IntWritable(1));
        mDriver.withOutput(new Text("India"), new IntWritable(1));
        mDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        rDriver.withInput(new Text("I"), values);
        rDriver.withOutput(new Text("I"), new IntWritable(2));
        rDriver.runTest();
    }

    @Test
    public void testMapReducer() throws IOException {
        mrDriver.addInput(new LongWritable(1), new Text("a day for a man"));
        mrDriver.addOutput(new Text("day"), new IntWritable(1));
        mrDriver.addOutput(new Text("for"), new IntWritable(1));
        mrDriver.addOutput(new Text("a"), new IntWritable(2));
        mrDriver.addOutput(new Text("man"), new IntWritable(1));
        mrDriver.runTest();
    }
}

