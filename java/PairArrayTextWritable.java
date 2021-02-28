import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairArrayTextWritable implements WritableComparable {
    public DoubleArrayWritable left;
    public Text right;


    public PairArrayTextWritable(DoubleArrayWritable left, Text right) {
        this.left = left;
        this.right = right;
    }


    public DoubleArrayWritable DoubleArrayWritable() {
        return left;
    }

    public Text getRight() {
        return right;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        left.write(dataOutput);
        right.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        left.readFields(dataInput);
        right.readFields(dataInput);
    }

    @Override
    public int compareTo(Object o) {
        PairArrayTextWritable other = (PairArrayTextWritable) o;
        int firstCompare = this.left.get()[0].compareTo(other.left.get()[0]);
        if (firstCompare != 0) {
            return firstCompare;
        }
        return this.right.compareTo(other.right);
    }

    public String toString() {
        return left.toString() + "," + right.toString();
    }
}
