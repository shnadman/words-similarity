import java.io.IOException;

import java.net.URI;
import java.util.*;


import com.google.common.collect.HashMultimap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CalcVectors {

    public static class MapperClass extends Mapper<Text, HashMapWritable<Text, DoubleWritable>, Text, HashMapWritable<Text, DoubleWritable>> {
        private final HashMultimap<String, String> dataSet = HashMultimap.create();
        private final HashMap<String, String> stemmedToOriginalMap = new HashMap<>();


        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            CountLexemeAndFeatures.fetchDatasetToMap(context, dataSet, stemmedToOriginalMap);
        }


        @Override
        public void map(Text key, HashMapWritable<Text, DoubleWritable> value, Context context) throws IOException, InterruptedException {
            if (!key.toString().contains("-")) {
                String originalWord = stemmedToOriginalMap.get(key.toString());
                if (dataSet.containsKey(originalWord)) {
                    Set<String> correspondingWords = dataSet.get(originalWord);
                    for (String word : correspondingWords) {
                        Text newKey = new Text(key.toString() + "@" + word);
                        context.write(newKey, value);
                    }
                }
                if (dataSet.containsValue(originalWord)) {
                    //Write every entry containing the current word (key) in the value
                    dataSet.entries().stream().filter(e -> e.getValue().equals(originalWord)).forEach(e -> {
                        Text newKey = new Text(e.getKey() + "@" + originalWord);
                        try {
                            context.write(newKey, value);
                        } catch (IOException | InterruptedException ioException) {
                            ioException.printStackTrace();
                        }
                    });
                }
            }
        }
    }


    public static class ReducerClass extends Reducer<Text, HashMapWritable<Text, DoubleWritable>, Text, PairArrayTextWritable> {
        private final HashMap<String, String> dataSet = new HashMap<>();
        private final HashMap<String, Double> featureOcc = new HashMap<>();


        @Override
        public void setup(ReducerClass.Context context) throws IOException, InterruptedException {
            fetchDatasetToMapLabel(context, dataSet);
            fetchFeatureOccToMap(context, featureOcc);
        }


        @Override
        public void reduce(Text key, Iterable<HashMapWritable<Text, DoubleWritable>> values, Context context) throws IOException, InterruptedException {
            try {
                HashMapWritable<Text, DoubleWritable> m1 = (HashMapWritable<Text, DoubleWritable>) values.iterator().next().clone();
                HashMapWritable<Text, DoubleWritable> m2 = (HashMapWritable<Text, DoubleWritable>) values.iterator().next().clone();
                double[] scores = ApplyCombinations.applyCombinations(featureOcc, m1, m2);
                DoubleArrayWritable converted = new DoubleArrayWritable(Arrays.stream(scores).mapToObj(DoubleWritable::new).toArray(DoubleWritable[]::new));
                String[] wordPair = key.toString().split("@");
                Text newKey = new Text(wordPair[0] + "," + wordPair[1]);
                Text label = new Text(dataSet.get(key.toString()));
                context.write(newKey, new PairArrayTextWritable(converted, label));
            } catch (Exception ignored) {

            }
        }


        private static void fetchDatasetToMapLabel(Reducer.Context context, HashMap<String, String> dataSet) throws IOException {
            FileSystem fileSystem = FileSystem.get(URI.create(EMR.ROOT_PATH), context.getConfiguration());
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path((EMR.DATASET_PATH)));
            LineIterator iterator = IOUtils.lineIterator(fsDataInputStream, "UTF-8");
            String nextLine = null;
            while (iterator.hasNext()) {
                nextLine = iterator.nextLine();
                String[] tokens = nextLine.split("\t");
                dataSet.put(tokens[0] + "@" + tokens[1], tokens[2]);
            }
            fsDataInputStream.close();
        }

        private static void fetchFeatureOccToMap(Reducer.Context context, HashMap<String, Double> featureOcc) throws IOException {
            FileSystem fileSystem = FileSystem.get(URI.create(EMR.FEATURES_PATH), context.getConfiguration());
            Path inDir = new Path((EMR.FEATURES_PATH));
            double totalOcc = 0;
            for (FileStatus inFile : fileSystem.listStatus(inDir)) {
                FSDataInputStream fsDataInputStream = fileSystem.open(inFile.getPath());
                LineIterator iterator = IOUtils.lineIterator(fsDataInputStream, "UTF-8");
                String nextLine = null;
                while (iterator.hasNext()) {
                    nextLine = iterator.nextLine();
                    String[] tokens = nextLine.split("\t");
                    double occ = Double.parseDouble(tokens[1]);
                    totalOcc += occ;
                    featureOcc.put(tokens[0], occ);
                }
                fsDataInputStream.close();
            }
            featureOcc.put("totalOcc", totalOcc);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = args[1];
        String outputPath = args[2];

        Job job = Job.getInstance(conf, "CalcVectors");

        job.setJarByClass(CalcVectors.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HashMapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairArrayTextWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        boolean completionStatus = job.waitForCompletion(true);
        System.exit(completionStatus ? 0 : 1);

    }
}

//import java.io.IOException;
//
//import java.net.URI;
//import java.util.*;
//
//
//import com.google.common.collect.HashMultimap;
//
//import opennlp.tools.stemmer.PorterStemmer;
//import org.apache.commons.io.IOUtils;
//import org.apache.commons.io.LineIterator;
//import org.apache.hadoop.conf.Configuration;
//
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//
//public class CalcVectors {
//
//    public static class MapperClass extends Mapper<Text, HashMapWritable<Text, DoubleWritable>, Text, HashMapWritable<Text, DoubleWritable>> {
//        private final HashMultimap<String, String> dataSet = HashMultimap.create();
//        private final HashMap<String, String> stemmedToOriginalMap = new HashMap<>();
//
//        @Override
//        public void setup(Context context) throws IOException, InterruptedException {
//            fetchDatasetToMap(context, dataSet, stemmedToOriginalMap);
//        }
//
//        @Override
//        public void map(Text key, HashMapWritable<Text, DoubleWritable> value, Context context) throws IOException, InterruptedException {
//            if (!key.toString().contains("-")) {
//                String originalWord = stemmedToOriginalMap.get(key.toString());
//                if (dataSet.containsKey(originalWord)) {
//                    Set<String> correspondingWords = dataSet.get(originalWord);
//                    for (String word : correspondingWords) {
//                        Text newKey = new Text(key.toString() + "@" + word);
//                        context.write(newKey, value);
//                    }
//                }
//                if (dataSet.containsValue(originalWord)) {
//                    //Write every entry containing the current word (key) in the value
//                    dataSet.entries().stream().filter(e -> e.getValue().equals(originalWord)).forEach(e -> {
//                        Text newKey = new Text(e.getKey() + "@" + originalWord);
//                        try {
//                            context.write(newKey, value);
//                        } catch (IOException | InterruptedException ioException) {
//                            ioException.printStackTrace();
//                        }
//                    });
//                }
//            }
//        }
//
//
//        private static void fetchDatasetToMap(Mapper.Context context, HashMultimap<String, String> dataSet, HashMap<String, String> stemmedToOriginial) throws IOException {
//            final PorterStemmer stemmer = new PorterStemmer();
//            FileSystem fileSystem = FileSystem.get(URI.create("/home/shnadman/IdeaProjects/dspAss3/src/main/resources/"), context.getConfiguration());
//            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/home/shnadman/IdeaProjects/dspAss3/src/main/resources/word-relatedness.txt"));
//            LineIterator iterator = IOUtils.lineIterator(fsDataInputStream, "UTF-8");
//            String nextLine = null;
//            while (iterator.hasNext()) {
//                nextLine = iterator.nextLine();
//                String[] tokens = nextLine.split("\t");
//                dataSet.put(tokens[0], tokens[1]);
//                if (stemmedToOriginial != null) {
//                    stemmedToOriginial.putIfAbsent(stemmer.stem(tokens[0]), tokens[0]);
//                    stemmedToOriginial.putIfAbsent(stemmer.stem(tokens[1]), tokens[1]);
//                }
//            }
//            fsDataInputStream.close();
//        }
//    }
//
//
//    public static class ReducerClass extends Reducer<Text, HashMapWritable<Text, DoubleWritable>, Text, PairArrayTextWritable> {
//        private final HashMap<String, String> dataSet = new HashMap<>();
//        private final HashMap<String, Double> featureOcc = new HashMap<>();
//
//
//        @Override
//        public void setup(ReducerClass.Context context) throws IOException, InterruptedException {
//            fetchDatasetToMapLabel(context, dataSet);
//            fetchFeatureOccToMap(context, featureOcc);
//        }
//
//
//        @Override
//        public void reduce(Text key, Iterable<HashMapWritable<Text, DoubleWritable>> values, Context context) throws IOException, InterruptedException {
//            try {
//                HashMapWritable<Text, DoubleWritable> m1 = (HashMapWritable<Text, DoubleWritable>) values.iterator().next().clone();
//                HashMapWritable<Text, DoubleWritable> m2 = (HashMapWritable<Text, DoubleWritable>) values.iterator().next().clone();
//                double[] scores = ApplyCombinations.applyCombinations(featureOcc, m1, m2);
//                DoubleArrayWritable converted = new DoubleArrayWritable(Arrays.stream(scores).mapToObj(DoubleWritable::new).toArray(DoubleWritable[]::new));
//                String[] wordPair = key.toString().split("@");
//                Text newKey = new Text(wordPair[0] + "," + wordPair[1]);
//                Text label = new Text(dataSet.get(key.toString()));
//                context.write(newKey, new PairArrayTextWritable(converted, label));
//            } catch (Exception e) {
//                System.out.println(e);
//            }
//        }
//
//
//        private static void fetchDatasetToMapLabel(Reducer.Context context, HashMap<String, String> dataSet) throws IOException {
//            FileSystem fileSystem = FileSystem.get(URI.create("/home/shnadman/IdeaProjects/dspAss3/src/main/resources/"), context.getConfiguration());
//            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/home/shnadman/IdeaProjects/dspAss3/src/main/resources/word-relatedness.txt"));
//            LineIterator iterator = IOUtils.lineIterator(fsDataInputStream, "UTF-8");
//            String nextLine = null;
//            while (iterator.hasNext()) {
//                nextLine = iterator.nextLine();
//                String[] tokens = nextLine.split("\t");
//                dataSet.put(tokens[0] + "@" + tokens[1], tokens[2]);
//            }
//            fsDataInputStream.close();
//        }
//
//
//        private static void fetchFeatureOccToMap(Reducer.Context context, HashMap<String, Double> featureOcc) throws IOException {
//            FileSystem fileSystem = FileSystem.get(URI.create("/home/shnadman/IdeaProjects/dspAss3/src/main/resources/"), context.getConfiguration());
//            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/home/shnadman/IdeaProjects/dspAss3/src/main/resources/FeaturesOcc/part-r-00000"));
//            LineIterator iterator = IOUtils.lineIterator(fsDataInputStream, "UTF-8");
//            String nextLine = null;
//            double totalOcc = 0;
//            while (iterator.hasNext()) {
//                nextLine = iterator.nextLine();
//                String[] tokens = nextLine.split("\t");
//                double occ = Double.parseDouble(tokens[1]);
//                totalOcc += occ;
//                featureOcc.put(tokens[0], occ);
//            }
//            featureOcc.put("totalOcc", totalOcc);
//            fsDataInputStream.close();
//        }
//
//
////        private static void fetchFeatureOccToMap(Reducer.Context context, HashMap<String, Double> featureOcc) throws IOException {
////            FileSystem fileSystem = FileSystem.get(URI.create(EMR.FEATURES_PATH), context.getConfiguration());
////            Path inDir = new Path((EMR.FEATURES_PATH));
////            double totalOcc = 0;
////            for (FileStatus inFile : fileSystem.listStatus(inDir)) {
////                FSDataInputStream fsDataInputStream = fileSystem.open(inFile.getPath());
////                LineIterator iterator = IOUtils.lineIterator(fsDataInputStream, "UTF-8");
////                String nextLine = null;
////                while (iterator.hasNext()) {
////                    nextLine = iterator.nextLine();
////                    String[] tokens = nextLine.split("\t");
////                    double occ = Double.parseDouble(tokens[1]);
////                    totalOcc += occ;
////                    featureOcc.put(tokens[0], occ);
////                }
////                fsDataInputStream.close();
////            }
////            featureOcc.put("totalOcc", totalOcc);
////        }
//    }
//
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String inputPath = "out1";
//        String outputPath = "stemDebug";
//
//        Job job = Job.getInstance(conf, "StepTwo");
//
//        job.setJarByClass(CalcVectors.class);
//        job.setMapperClass(MapperClass.class);
//        job.setReducerClass(ReducerClass.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(HashMapWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(PairArrayTextWritable.class);
//        FileInputFormat.addInputPath(job, new Path(inputPath));
//        FileOutputFormat.setOutputPath(job, new Path(outputPath));
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        //job.setOutputFormatClass(TextOutputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        boolean completionStatus = job.waitForCompletion(true);
//        System.exit(completionStatus ? 0 : 1);
//
//    }
//}
