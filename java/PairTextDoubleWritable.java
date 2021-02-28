import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairTextDoubleWritable implements WritableComparable {
    public Text left;
    public DoubleWritable right;

    public PairTextDoubleWritable() {
        this.left = new Text("");
        this.right = new DoubleWritable(0);
    }

    public PairTextDoubleWritable(Text left, DoubleWritable right) {
        this.left = left;
        this.right = right;
    }

    public PairTextDoubleWritable(Text left, Double right) {
        this.left = left;
        this.right = new DoubleWritable(right);
    }


    public Text getLeft() {
        return left;
    }

    public DoubleWritable getRight() {
        return right;
    }

    public void set(Text left, Double right) {
        this.left = left;
        this.right = new DoubleWritable(right);
        ;
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
        PairTextDoubleWritable other = (PairTextDoubleWritable) o;
        int firstCompare = this.left.compareTo(other.left);
        if (firstCompare != 0) {
            return firstCompare;
        }
        return this.right.compareTo(other.right);
    }

    public String toString() {
        return left.toString() + "_" + right.toString();
    }
}
