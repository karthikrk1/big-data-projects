package util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * Created by Karthik Ramakrishnan on 12/16/17.
 */
public class TextPair implements WritableComparable<TextPair>{
    private Text first;
    private Text second;

    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public TextPair(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return this.first;
    }

    public Text getSecond() {
        return this.second;
    }

    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    public int compareTo(TextPair textPair) {
        int cmp = first.compareTo(textPair.first);
        if(cmp!=0) {
            return cmp;
        }
        return second.compareTo(textPair.second);
    }

    @Override
    public int hashCode() {
        return first.hashCode()*31991 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof TextPair) {
            TextPair tPair = (TextPair) obj;
            return first.equals(tPair.first) && second.equals(tPair.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }
}
