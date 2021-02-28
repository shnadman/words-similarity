import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.util.*;
import java.util.function.BiFunction;

public class ApplyCombinations {

    private static final double[] scores = new double[24];
    private static double totalFeaturesLexeme1;
    private static double totalFeaturesLexeme2;
    private static double totalOcc;

    public static double[] applyCombinations(HashMap<String, Double> featureOcc, HashMapWritable<Text, DoubleWritable> m1, HashMapWritable<Text, DoubleWritable> m2) {
        totalFeaturesLexeme1 = m1.values().stream().mapToDouble(DoubleWritable::get).sum();
        totalFeaturesLexeme2 = m2.values().stream().mapToDouble(DoubleWritable::get).sum();
        totalOcc = featureOcc.get("totalOcc");

        for (int j = 0; j < 4; j++) {
            Vector[] vectors = buildVectors(m1, m2, featureOcc, j);
            Vector cloned1 = vectors[0].clone();
            Vector cloned2 = vectors[1].clone();

            for (int k = 0; k < 6; k++, cloned1 = vectors[0].clone(), cloned2 = vectors[1].clone()) {
                Double score = applyVectorSimilarityMeasure(cloned1, cloned2, k);
                scores[(k + 1) * (j + 1) - 1] = score;
            }
        }
        return scores;
    }


    private static Vector[] buildVectors(HashMapWritable<Text, DoubleWritable> m1, HashMapWritable<Text, DoubleWritable> m2, HashMap<String, Double> featureOcc, int associationWithContext) {
        Vector[] computedVectors = new Vector[2];
        final Text key = new Text("");
        int vectorSize = featureOcc.size();
        double[] v1 = new double[vectorSize];
        double[] v2 = new double[vectorSize];
        int i = 0;
        for (Map.Entry<String, Double> entry : featureOcc.entrySet()) {
            if (!entry.getKey().equals("totalOcc")) {
                key.set(entry.getKey());
                DoubleWritable val1 = m1.get(key);
                DoubleWritable val2 = m2.get(key);
                computeVectorCoordinate(associationWithContext, v1, entry.getValue(), totalFeaturesLexeme1, val1, i, totalOcc);
                computeVectorCoordinate(associationWithContext, v2, entry.getValue(), totalFeaturesLexeme2, val2, i, totalOcc);
                i++;
            }
        }

        DenseVector vecOne = new DenseVector(v1);
        DenseVector vecTwo = new DenseVector(v2);

        computedVectors[0] = vecOne;
        computedVectors[1] = vecTwo;
        return computedVectors;
    }

    private static void computeVectorCoordinate(int associationWithContext, double[] v, double totalCurrentFeature, double totalFeaturesGivenLexeme, DoubleWritable val, int i, double totalOcc) {
        double probF = (totalCurrentFeature / totalOcc);
        double probL = (totalFeaturesGivenLexeme / totalOcc);
        //Special case for T-test association, we don't assign 0 straight away
        if (val == null && associationWithContext == 3) {
            v[i] = (-probF * probL) / (Math.sqrt(probF * probL));
        } else if (val == null) {
            v[i] = 0;
        } else {
            double probLF = val.get() / totalOcc;
            switch (associationWithContext) {
                //assocFreq(l, f)=count(l, f)
                case 0:
                    v[i] = val.get();
                    break;
                //assocProb(l, f)=P(f|l)
                case 1:
                    v[i] = val.get() / totalFeaturesGivenLexeme;
                    break;
                //assocPMI(l, f)=(log2P(l, f)/P(l)P(f))
                case 2:
                    v[i] = log2(probLF / (probL * probF));
                    break;
                //assoct−test(l, f)=(P(l, f)−P(l)P(f))/√P(l)P(f)
                case 3:
                    v[i] = (probLF - probF * probL) / (Math.sqrt(probF * probL));
                    break;
            }
        }
    }


    static List<BiFunction<Vector, Vector, Double>> vectorSimilarity = Arrays.asList(
            DistanceMeasures::manhattan, //distManhattan(l1,l2)=N∑i=1|l1i−l2i|
            DistanceMeasures::euclidean, //distEuclidean(l1,l2)=√√√√N∑i=1(l1i−l2i)2
            DistanceMeasures::cosine, //simCosine(l1,l2)=∑Ni=1l1i∗l2i√∑Ni=1l21i√∑Ni=1l2
            DistanceMeasures::jaccard, //simJaccard(l1,l2)=∑Ni=1min(l1i,l2i)∑Ni=1max(l1i,l2i)
            DistanceMeasures::dice, //simDice(l1,l2)=2∗∑Ni=1min(l1i,l2i)∑Ni=1(l1i,l2i)
            DistanceMeasures::jensenShannonDivergence  //imJS(l1,l2)=D(l1||l1+l2/2)+D(l2||l1+l2/2)
    );


    private static Double applyVectorSimilarityMeasure(Vector v1, Vector v2, int measure) {
        return vectorSimilarity.get(measure).apply(v1, v2);
    }

    private static final double LOG2 = Math.log(2);

    public static double log2(double x) {
        return Math.log(x) / LOG2;
    }
}
