import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TextArray extends ArrayWritable {

    public TextArray(Text[] values) {
        super(Text.class, values);
    }

    @Override
    public Text[] get() {
        return (Text[]) super.get();
    }

    @Override
    public String toString() {
        Text[] values = get();
        StringBuilder strings = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            strings.append(" ").append(values[i].toString());
        }
        return strings.toString();
    }


}