package bigramtest;

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

import util.TextPair;
import bigram.BigramMapper;
import bigram.BigramReducer;

/**
 * Created by Karthik Ramakrishnan on 12/16/17.
 */

public class BigramTester {
    MapDriver<LongWritable, Text, TextPair, IntWritable> mapDriver;
    ReduceDriver<TextPair, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, TextPair, IntWritable, Text, IntWritable> mrDriver;

    @Before
    public void setUp(){
        BigramMapper map = new BigramMapper();
        mapDriver = new MapDriver<LongWritable, Text, TextPair, IntWritable>();
        mapDriver.setMapper(map);

        BigramReducer reduce= new BigramReducer();
        reduceDriver = new ReduceDriver<TextPair, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(reduce);

        mrDriver = new MapReduceDriver<LongWritable, Text, TextPair, IntWritable, Text, IntWritable>();
        mrDriver.setMapper(map);
        mrDriver.setReducer(reduce);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("I read a book about the history of India."));
        mapDriver.withOutput(new TextPair("I", "read"), new IntWritable(1));
        mapDriver.withOutput(new TextPair("read", "a"), new IntWritable(1));
        mapDriver.withOutput(new TextPair("a", "book"), new IntWritable(1));
        mapDriver.withOutput(new TextPair("book", "about"), new IntWritable(1));
        mapDriver.withOutput(new TextPair("about", "the"), new IntWritable(1));
        mapDriver.withOutput(new TextPair("the", "history"), new IntWritable(1));
        mapDriver.withOutput(new TextPair("history", "of"), new IntWritable(1));
        mapDriver.withOutput(new TextPair("of", "India"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> vals = new ArrayList<IntWritable>();
        vals.add(new IntWritable(1));
        vals.add(new IntWritable(1));
        reduceDriver.withInput(new TextPair("I", "read"), vals);
        reduceDriver.withOutput(new Text("(I, read)"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mrDriver.addInput(new LongWritable(1), new Text("Hello World we meet again we meet"));
        mrDriver.addOutput(new Text("(Hello, World)"), new IntWritable(1));
        mrDriver.addOutput(new Text("(World, we)"), new IntWritable(1));
        mrDriver.addOutput(new Text("(meet, again)"), new IntWritable(1));
        mrDriver.addOutput(new Text("(again, we)"), new IntWritable(1));
        mrDriver.addOutput(new Text("(we, meet)"), new IntWritable(2));
        mrDriver.runTest();
    }
}
