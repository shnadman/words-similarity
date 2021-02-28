import org.apache.commons.io.FileUtils;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.tools.mail.ErrorInQuitException;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Random;

import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.StringToNominal;


public class Classification {
    private static final String ARFF_PATH = "data.arff";
    public static final String PREPROCESSED_PATH = "outputfinal"; //Local

    public static void main(String[] args) throws Exception {
        convertToArff(PREPROCESSED_PATH);
        DataSource source = new DataSource(ARFF_PATH);
        Instances dataset = source.getDataSet();

        /*WEKA classifiers cant handle string values so we turn them into nominal ones by using this Filter**/
        StringToNominal filter = new StringToNominal();
        filter.setAttributeRange("1-2");
        filter.setInputFormat(dataset);
        Instances filteredDataset = Filter.useFilter(dataset, filter);

        /*classIndex specifies to WEKA which of the attributes it should consider as the label={similar,not-similar},
         usually it's the last one*/
        int classIndex = filteredDataset.numAttributes() - 1;
        filteredDataset.setClassIndex(classIndex);

        J48 classifier = new J48();
        int fold = 10;
        int seed = 1;
        Random rand = new Random(seed);
        Instances randData = new Instances(filteredDataset);
        randData.randomize(rand);
        if (randData.classAttribute().isNominal())
            randData.stratify(fold);
        double totalCorrect = 0;
        double totalRecall = 0;
        double totalPrecision = 0;
        for (int n = 0; n < fold; n++) {
            Evaluation eval = new Evaluation(randData);
            Instances train = randData.trainCV(fold, n);
            Instances test = randData.testCV(fold, n);
            classifier.buildClassifier(train);
            eval.evaluateModel(classifier, test);

            /*Double check this but I think classIndex refers to the index of the "positive" value of the label attribiute
             * In our case {True,False} that would be index 0**/
            totalRecall += eval.recall(0);
            totalPrecision += eval.precision(0);
            totalCorrect += eval.pctCorrect();
            ;
            System.out.println("The " + n + "th cross validation:" + eval.toSummaryString() + "\n" + eval.toClassDetailsString());
            System.out.println("=========================================================================================================");

        }
        double averageRecall = totalRecall / fold;
        double averagePrecision = totalPrecision / fold;
        double f1Measure = (2 * averagePrecision * averageRecall) / (averageRecall + averagePrecision);
        System.out.println("Average correction rate of " + fold + " cross validation: " + totalCorrect / fold + "\n");
        System.out.println("Average recall of " + fold + " cross validation: " + averageRecall + "\n");
        System.out.println("Average precision rate of " + fold + " cross validation: " + averagePrecision + "\n");
        System.out.println("F1 measure of " + fold + " cross validation: " + f1Measure);
        //TODO F1 Score is some computation with the recall and precision we calculated

    }

    public static void convertToArff(String dirPath) throws IOException {
        File target = new File(ARFF_PATH);
        FileUtils.writeStringToFile(target, ARFF_META);
        File folder = new File(dirPath);
        for (File currFile : Objects.requireNonNull(folder.listFiles())) {
            //File currFile = new File(filePath);
            String data = FileUtils.readFileToString(currFile);
            FileUtils.writeStringToFile(target, data, true);
        }
    }


    /**
     * We chose to inject the WEKA header meta data in this format since it's declarative and one can
     * infer the structure needed for the classification with a simple glance
     */
    static private final String ARFF_META =
            "@relation similarity\n" +
                    "@attribute first-word string\n" +
                    "@attribute second-word string\n" +
                    "@attribute s1 numeric\n" +
                    "@attribute s2 numeric\n" +
                    "@attribute s3 numeric\n" +
                    "@attribute s4 numeric\n" +
                    "@attribute s5 numeric\n" +
                    "@attribute s6 numeric\n" +
                    "@attribute s7 numeric\n" +
                    "@attribute s8 numeric\n" +
                    "@attribute s9 numeric\n" +
                    "@attribute s10 numeric\n" +
                    "@attribute s11 numeric\n" +
                    "@attribute s12 numeric\n" +
                    "@attribute s13 numeric\n" +
                    "@attribute s14 numeric\n" +
                    "@attribute s15 numeric\n" +
                    "@attribute s16 numeric\n" +
                    "@attribute s17 numeric\n" +
                    "@attribute s18 numeric\n" +
                    "@attribute s19 numeric\n" +
                    "@attribute s20 numeric\n" +
                    "@attribute s21 numeric\n" +
                    "@attribute s22 numeric\n" +
                    "@attribute s23 numeric\n" +
                    "@attribute s24 numeric\n" +
                    "@attribute similar {True, False}\n" +
                    "@data\n";

}


