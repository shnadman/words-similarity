import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairTexts implements Writable {
    public Text left;
    public Text right;

    public PairTexts() {
        this.left = new Text("");
        this.right = new Text("");
    }


    public PairTexts(String left, String right) {
        this.left = new Text(left);
        this.right = new Text(right);
    }

    public Text getLeft() {
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

    public String toString() {
        return left.toString() + "_" + right.toString();
    }

}