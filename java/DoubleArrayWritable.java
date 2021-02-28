import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.Arrays;

public class DoubleArrayWritable extends ArrayWritable {

    public DoubleArrayWritable(DoubleWritable[] values) {
        super(DoubleWritable.class, values);
    }
    
    @Override
    public DoubleWritable[] get() {
        return (DoubleWritable[]) super.get();
    }

    @Override
    public String toString() {
        DoubleWritable[] values = get();
        StringBuilder strings = new StringBuilder();
        for (DoubleWritable value : values) {
            strings.append(",").append(value.toString());
        }
        return strings.toString();
    }

}