package wctwo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Karthik Ramakrishnan on 12/16/17.
 */
public class MinReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int min=0;
        Configuration conf = context.getConfiguration();
        try {
            min = Integer.parseInt(conf.get("reducer.minCount"));
        }
        catch(Exception e) {

        }
        int count = 0;
        for(IntWritable val: values) {
            count += val.get();
        }
        if(count>=min) {
            context.write(key, new IntWritable(count));
        }
    }
}
