import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;


import com.google.common.collect.HashMultimap;
import opennlp.tools.stemmer.PorterStemmer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountLexemeAndFeatures {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, PairTextDoubleWritable> {
        private final Text dataSetWord = new Text("");
        private final Text concatenatedFeature = new Text("");
        private final Text emptyText = new Text("");
        private final PorterStemmer stemmer = new PorterStemmer();
        private final HashMultimap<String, String> dataSet = HashMultimap.create();
        private String[] splittedRecord;


        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            fetchDatasetToMap(context, dataSet, null);
        }

        //went	leo/NNP/nsubj/2 went/VBD/ROOT/0 to/TO/prep/2 harvard/NNP/pobj/3	19	1938,1	1953,3	1959,4	1961,1	1968,1	1971,3	1986,1	1987,3	1991,1	1993,1
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            System.out.println(line);
            splittedRecord = line.toString().split("\t");
            System.out.println(Arrays.toString(splittedRecord));
            if (splittedRecord.length >= 3) {
                Double occurrences = Double.parseDouble(splittedRecord[2]);
                String headWord = stemmer.stem(splittedRecord[0]);
                List<String[]> wordDepsPairs = Arrays.stream(splittedRecord[1].split(" ")).map(ngram -> new String[]{ngram.split("/")[0], ngram.split("/")[2]}).collect(Collectors.toList());
                for (String[] wordDepPair : wordDepsPairs) {
                    String coWord = wordDepPair[0];
                    String depLabel = wordDepPair[1];
                    if (!depLabel.equals("ROOT") && (dataSet.containsKey(coWord) || dataSet.containsValue(coWord))) {
                        concatenatedFeature.set(headWord + "-" + depLabel);
                        dataSetWord.set(stemmer.stem(coWord));
                        context.write(dataSetWord, new PairTextDoubleWritable(concatenatedFeature, occurrences)); //key - Lexeme
                        context.write(concatenatedFeature, new PairTextDoubleWritable(emptyText, occurrences));//key - Feature
                    }
                }
            }
        }


    }

    public static class ReducerClass extends Reducer<Text, PairTextDoubleWritable, Text, HashMapWritable<Text, DoubleWritable>> {
        @Override
        public void reduce(Text key, Iterable<PairTextDoubleWritable> values, Context context) throws IOException, InterruptedException {
            if (key.toString().contains("-")) {
                handleFeature(key, values, context);
            } else {
                handleLexeme(key, values, context);
            }
        }

        private void handleFeature(Text key, Iterable<PairTextDoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalOcc = 0;
            HashMapWritable<Text, DoubleWritable> occurrences = new HashMapWritable<>();
            for (PairTextDoubleWritable value : values) {
                totalOcc += value.getRight().get();
            }
            occurrences.put(key, new DoubleWritable(totalOcc));
            context.write(key, occurrences);
        }

        private void handleLexeme(Text key, Iterable<PairTextDoubleWritable> values, Context context) throws IOException, InterruptedException {
            HashMapWritable<Text, DoubleWritable> occurrences = new HashMapWritable<>();
            for (PairTextDoubleWritable value : values) {
                Text datasetKey = new Text(value.getLeft());
                DoubleWritable currCount = occurrences.get(datasetKey);
                if (currCount == null) {
                    occurrences.put(datasetKey, value.getRight());
                } else {
                    DoubleWritable newVal = new DoubleWritable(currCount.get() + value.getRight().get());
                    occurrences.put(datasetKey, newVal);
                }
            }
            context.write(key, occurrences);
        }
    }

    public static void fetchDatasetToMap(Mapper.Context context, HashMultimap<String, String> dataSet, HashMap<String, String> stemmedToOriginial) throws IOException {
        final PorterStemmer stemmer = new PorterStemmer();
        FileSystem fileSystem = FileSystem.get(URI.create(EMR.ROOT_PATH), context.getConfiguration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path((EMR.DATASET_PATH)));
        LineIterator iterator = IOUtils.lineIterator(fsDataInputStream, "UTF-8");
        String nextLine = null;
        while (iterator.hasNext()) {
            nextLine = iterator.nextLine();
            String[] tokens = nextLine.split("\t");
            dataSet.put(tokens[0], tokens[1]);
            if (stemmedToOriginial != null) {
                stemmedToOriginial.putIfAbsent(stemmer.stem(tokens[0]), tokens[0]);
                stemmedToOriginial.putIfAbsent(stemmer.stem(tokens[1]), tokens[1]);
            }
        }
        fsDataInputStream.close();
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = args[1];
        String outputPath = args[2];

        Job job = Job.getInstance(conf, "CountLexemeAndFeatures");

        job.setJarByClass(CountLexemeAndFeatures.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairTextDoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HashMapWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        boolean completionStatus = job.waitForCompletion(true);
        System.exit(completionStatus ? 0 : 1);

    }
}
