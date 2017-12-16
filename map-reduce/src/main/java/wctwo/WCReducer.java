package wctwo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Karthik Ramakrishnan on 12/16/17.
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val: value) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
