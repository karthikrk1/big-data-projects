package bigram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import util.TextPair;
/**
 * Created by Karthik Ramakrishnan on 12/16/17.
 */
public class BigramReducer extends Reducer<TextPair, IntWritable, Text, IntWritable>{
    @Override
    public void reduce(TextPair tPair, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val: vals) {
            sum += val.get();
        }
        context.write(new Text(tPair.toString()), new IntWritable(sum));
    }
}
