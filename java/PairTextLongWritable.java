import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairTextLongWritable implements WritableComparable {
    public Text left;
    public LongWritable right;

    public PairTextLongWritable() {
        this.left = new Text("");
        this.right = new LongWritable(0);
    }

    public PairTextLongWritable(Text left, LongWritable right) {
        this.left = left;
        this.right = right;
    }

    public PairTextLongWritable(Text left, Long right) {
        this.left = left;
        this.right = new LongWritable(right);
    }

    public PairTextLongWritable(String left, Long right) {
        this.left = new Text(left);
        this.right = new LongWritable(right);
    }

    public PairTextLongWritable(String left, LongWritable right) {
        this.left = new Text(left);
        this.right = right;
    }

    public Text getLeft() {
        return left;
    }

    public LongWritable getRight() {
        return right;
    }

    public void set(Text left, Long right) {
        this.left = left;
        this.right = new LongWritable(right);
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
        PairTextLongWritable other = (PairTextLongWritable) o;
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
