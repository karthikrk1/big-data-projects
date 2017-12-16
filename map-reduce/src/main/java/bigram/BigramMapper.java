package bigram;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.TextPair;
/**
 * Created by Karthik Ramakrishnan on 12/16/17.
 */
public class BigramMapper extends Mapper<LongWritable, Text, TextPair, IntWritable>{
    private Text currWord=new Text();
    private Text lastWord=null;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        line = line.replace(",", "");
        line = line.replace(".", "");

        for(String word: line.split(" ")) {
            if(lastWord==null) {
                lastWord = new Text(word);
            }
            else {
                currWord.set(word);
                context.write(new TextPair(lastWord, currWord), new IntWritable(1));
                lastWord.set(currWord.toString());
            }
        }
    }
}
