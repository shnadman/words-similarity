import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.parameters.Parameter;
import org.apache.mahout.math.CardinalityException;
import org.apache.mahout.math.Vector;

public final class DistanceMeasures {

    /**
     * This class implements a cosine distance metric by dividing the dot product of two vectors by the product of their
     * lengths
     */
    public static double cosine(Vector v1, Vector v2) {
        if (v1.size() != v2.size()) {
            throw new CardinalityException(v1.size(), v2.size());
        }
        double lengthSquaredv1 = v1.getLengthSquared();
        double lengthSquaredv2 = v2.getLengthSquared();

        double dotProduct = v2.dot(v1);
        double denominator = Math.sqrt(lengthSquaredv1) * Math.sqrt(lengthSquaredv2);

        // correct for floating-point rounding errors
        if (denominator < dotProduct) {
            denominator = dotProduct;
        }

        return 1.0 - dotProduct / denominator;
    }

    public static double euclidean(Vector v1, Vector v2) {
        return Math.sqrt(v2.getDistanceSquared(v1));
    }


    public static double manhattan(Vector v1, Vector v2) {
        if (v1.size() != v2.size()) {
            throw new CardinalityException(v1.size(), v2.size());
        }
        double result = 0;
        Vector vector = v1.minus(v2);
        Iterator<Vector.Element> iter = vector.iterateNonZero();
        // this contains all non zero elements between the two
        while (iter.hasNext()) {
            Vector.Element e = iter.next();
            result += Math.abs(e.get());
        }
        return result;
    }

    public static double jaccard(Vector v1, Vector v2) {
        if (v1.size() != v2.size()) {
            throw new CardinalityException(v1.size(), v2.size());
        }
        double numerator = 0;
        double denominator = 0;
        for (int i = 0; i < v1.size(); i++) {
            numerator += Math.min(v1.get(i), v2.get(i));
            denominator += Math.max(v1.get(i), v2.get(i));
        }
        return numerator / denominator;
    }


    public static double dice(Vector v1, Vector v2) {
        if (v1.size() != v2.size()) {
            throw new CardinalityException(v1.size(), v2.size());
        }
        double numerator = 0;
        double denominator = 0;
        for (int i = 0; i < v1.size(); i++) {
            numerator += Math.min(v1.get(i), v2.get(i));
            denominator += v1.get(i) + v2.get(i);
        }
        return 2 * numerator / denominator;
    }


    public static double jensenShannonDivergence(Vector p, Vector q) {
        Vector m = p.plus(q).divide(2);
        return (KullbackLeiblerDivergence(p, m) + KullbackLeiblerDivergence(q, m)) / 2;
    }

    private static double KullbackLeiblerDivergence(Vector p, Vector q) {
        boolean intersection = false;
        double kl = 0.0;

        for (int i = 0; i < p.size(); i++) {
            if (p.get(i) != 0.0 && q.get(i) != 0.0) {
                intersection = true;
                kl += p.get(i) * Math.log(p.get(i) / q.get(i));
            }
        }

        if (intersection) {
            return kl;
        } else {
            return Double.POSITIVE_INFINITY;
        }
    }


}
