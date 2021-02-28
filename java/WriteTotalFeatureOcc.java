import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WriteTotalFeatureOcc {

    public static class MapperClass extends Mapper<Text, HashMapWritable<Text, DoubleWritable>, Text, DoubleWritable> {
        @Override
        public void map(Text key, HashMapWritable<Text, DoubleWritable> singletonMap, Context context) throws IOException, InterruptedException {
            if (key.toString().contains("-")) {
                context.write(key, singletonMap.get(key));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = args[1];
        String outputPath = "s3://dsp-ass-3/data/FeaturesOcc/";

        Job job = Job.getInstance(conf, "WriteTotalFeatureOcc");
        job.setJarByClass(WriteTotalFeatureOcc.class);
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        boolean completionStatus = job.waitForCompletion(true);
        System.exit(completionStatus ? 0 : 1);
    }
}
