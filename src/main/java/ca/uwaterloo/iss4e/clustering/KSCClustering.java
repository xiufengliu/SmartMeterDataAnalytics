package ca.uwaterloo.iss4e.clustering;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Created by xiuli on 5/30/16.
 */
public class KSCClustering implements Serializable {

  private static final Logger log = Logger.getLogger(KSCClustering.class);


  private static final String pointDelim = ",";
  private static final String keyDelim = "\\|";


  public static class CentroidPartitioner extends Partitioner {


    private int numParts;

    public CentroidPartitioner(int numParts) {
      this.numParts = numParts;
    }

    @Override
    public int numPartitions() {
      return numParts;
    }

    public int getPartition(Object key) {
      Integer k = (Integer) key;
      return k.intValue() % numParts;
    }
  }

  protected static JavaPairRDD<Long, Double[]> loadPoints(JavaSparkContext sc, String pointInputDir, int minPartitions) {
    return sc.textFile(pointInputDir, minPartitions)
        .mapToPair(new PairFunction<String, Long, Double[]>() {
          public Tuple2<Long, Double[]> call(String line) throws Exception {//1000 111201|2.1,1.2,3.1...,4.3
            String[] fields = line.split(keyDelim);
            String[] points = fields[1].split(pointDelim);
            Double[] pointArray = new Double[points.length];
            for (int i = 0; i < points.length; ++i) {
              pointArray[i] = Double.parseDouble(points[i]);
            }
            return new Tuple2<Long, Double[]>(Long.parseLong(fields[0]), pointArray);//(ID, hourlyreadingArray)
          }
        });
  }


  protected static JavaPairRDD<Integer, Double[]> loadCentroids(JavaSparkContext sc, String centroidInputDir) {
    return sc.textFile(centroidInputDir)
        .mapToPair(new PairFunction<String, Integer, Double[]>() {
          public Tuple2<Integer, Double[]> call(String line) throws Exception {
            String[] fields = line.split(keyDelim);
            int k = Integer.parseInt(fields[0]);
            String[] points = fields[1].split(pointDelim);
            Double[] pointArray = new Double[points.length];
            for (int i = 0; i < points.length; ++i) {
              pointArray[i] = Double.parseDouble(points[i]);
            }
            return new Tuple2<Integer, Double[]>(k, pointArray);
          }
        });
  }

  protected static Map<Integer, Double[]> initCentroids(JavaPairRDD<Integer, Double[]> allCentroids, final int K) {
    return allCentroids.filter(new Function<Tuple2<Integer, Double[]>, Boolean>() {
      public Boolean call(Tuple2<Integer, Double[]> centroids) throws Exception {
        return centroids._1().intValue() <= K;
      }
    }).collectAsMap();
  }

  public static Double[] rotateArray(final Double[] array, final int shiftNum) //Will make a new copy
  {
    Double[] result = new Double[array.length];
    if (shiftNum == 0 || Math.abs(shiftNum) >= array.length) {
      System.arraycopy(array, 0, result, 0, array.length);
      return array;
    }

    if (shiftNum > 0) {//left shift
      System.arraycopy(array, shiftNum, result, 0, array.length - shiftNum);
      System.arraycopy(array, 0, result, array.length - shiftNum, shiftNum);
    } else {//right shift
      System.arraycopy(array, 0, result, Math.abs(shiftNum), array.length + shiftNum);
      System.arraycopy(array, array.length + shiftNum, result, 0, Math.abs(shiftNum));
    }
    return result;
  }

  public static Double[] chopEdges(final Double[] array, int desiredLength) {
    if (array.length > desiredLength) {
      int startPos = (int) Math.floor(1.0 * (array.length - desiredLength) / 2.0);
      Double[] result = new Double[desiredLength];
      System.arraycopy(array, startPos, result, 0, desiredLength);
      return result;
    } else {
      return array;
    }
  }

  /**
   * Calculate the euclidean distances between two points represented as arrays.
   */
  protected static double euclideanDistance(final Double[] p1, final Double[] p2) {
    double sum = 0;
    if (p1.length != p2.length) {
      throw new RuntimeException("Both points should contain the same number of values.");
    }
    for (int i = 0; i < p1.length; i++) {       //ignore missing values
      if (!Double.isNaN(p2[i]) && !Double.isNaN(p1[i]))
        sum += (p2[i] - p1[i]) * (p2[i] - p1[i]);
    }
    return Math.sqrt(sum);
  }

  public static double cosineSimilarity(Double[] vectorA, Double[] vectorB) {
    if (vectorA.length != vectorB.length) {
      throw new RuntimeException("Both points should contain the same number of values.");
    }
    double dotProduct = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < vectorA.length; i++) {
      dotProduct += vectorA[i] * vectorB[i];
      normA += Math.pow(vectorA[i], 2);
      normB += Math.pow(vectorB[i], 2);
    }
    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
  }

  protected static double[] KscDistance(final Double[] point, final Double[] centroid, int maxShiftNum) {// P1 will be the shifted Array
    double minDist = Double.MAX_VALUE;
    int actualShift = 0;
    for (int shift = -maxShiftNum; shift < maxShiftNum + 1; ++shift) {
      Double[] shiftedArray = rotateArray(point, shift); //Will make a new copy
      Double[] choppedArray = chopEdges(shiftedArray, centroid.length);
      normalize(choppedArray);
      double dist = euclideanDistance(choppedArray, centroid);
      //double dist = cosineSimilarity(choppedArray, centroid);
      if (dist < minDist) {
        minDist = dist;
        actualShift = shift;
      }
    }
    return new double[]{minDist, 1.0 * actualShift};
  }

  public static void normalize(Double[] array) {// Will changed the value of point
    double sum = 0.0;
    for (int i = 0; i < array.length; ++i) {
      sum += array[i].doubleValue();
    }
    if (sum != 0.0) {
      for (int i = 0; i < array.length; ++i) {
        array[i] = array[i].doubleValue() / sum;
      }
    }
  }

  protected static JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assign(final Broadcast<Map<Integer, Double[]>> centroids,
                                                                                final JavaPairRDD<Long, Double[]> points,
                                                                                final int maxShiftNum) {
    return points.mapToPair(new PairFunction<Tuple2<Long, Double[]>, Integer, Tuple3<Long, Double[], Integer>>() {
      public Tuple2<Integer, Tuple3<Long, Double[], Integer>> call(Tuple2<Long, Double[]> point) throws Exception {//(ID, pointArray)
        double closestDistance = Double.MAX_VALUE;
        int actualShift = 0;
        int centroidLength = 0;
        int closestCentroidID = -1;

        for (Map.Entry<Integer, Double[]> entry : centroids.getValue().entrySet()) {
          int centroidID = entry.getKey();
          Double[] centroid = entry.getValue();
          double[] kscDist = KscDistance(point._2(), centroid, maxShiftNum);
          if (kscDist[0] < closestDistance) {
            closestDistance = kscDist[0];
            closestCentroidID = centroidID;
            actualShift = (int) kscDist[1];
            centroidLength = centroid.length;
          }
        }
        Double[] shiftedArray = rotateArray(point._2(), actualShift); //Will make a new copy
        Double[] newArray = chopEdges(shiftedArray, centroidLength); // 24hours
        normalize(newArray); // normed to 1

        return new Tuple2<Integer, Tuple3<Long, Double[], Integer>>(closestCentroidID,
            new Tuple3<Long, Double[], Integer>(point._1(), newArray, 1)); //(ID, pointArray, 1)
      }
    });
  }


  protected static JavaPairRDD<Integer, Double[]> calculateNewCentroids(final JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assigned,
                                                                        final Broadcast<Map<Integer, Double[]>> oldCentroids,
                                                                        final Accumulator<Integer> converged, final double delta) {
    int numCentroids = oldCentroids.getValue().size();
    CentroidPartitioner partition = new CentroidPartitioner(numCentroids);

    return assigned.reduceByKey(partition, new Function2<Tuple3<Long, Double[], Integer>,
        Tuple3<Long, Double[], Integer>,
        Tuple3<Long, Double[], Integer>>() {
      public Tuple3<Long, Double[], Integer> call(Tuple3<Long, Double[], Integer> x,
                                                  Tuple3<Long, Double[], Integer> y) throws Exception {
        Double[] p1 = x._2();
        Double[] p2 = y._2();

        Double newPoint[] = new Double[p1.length];
        for (int i = 0; i < p1.length; ++i) {
          newPoint[i] = p1[i] + p2[i];
        }
        int newSum = x._3() + y._3();
        return new Tuple3<Long, Double[], Integer>(-1L, newPoint, newSum);
      }
    }).mapToPair(new PairFunction<Tuple2<Integer, Tuple3<Long, Double[], Integer>>, Integer, Double[]>() {
      public Tuple2<Integer, Double[]> call(Tuple2<Integer, Tuple3<Long, Double[], Integer>> x) throws Exception {
        Integer centroidID = x._1();
        Double[] partial = x._2()._2();
        int n = x._2()._3();
        Double[] newCentroid = new Double[partial.length];
        for (int i = 0; i < partial.length; ++i) {
          newCentroid[i] = partial[i] / n;
        }
        Double[] oldCentroid = oldCentroids.getValue().get(centroidID);
        if (euclideanDistance(newCentroid, oldCentroid) > delta) {
          converged.add(1);
        }
        return new Tuple2<Integer, Double[]>(centroidID, newCentroid);
      }
    });

  }

  protected static JavaPairRDD<String, String> tagPoints(final JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assigned) //assignedCentroidID, (ID, pointArray, 1)
  {
    return assigned.mapToPair(new PairFunction<Tuple2<Integer, Tuple3<Long, Double[], Integer>>, Long, Integer>() {
      public Tuple2<Long, Integer> call(Tuple2<Integer, Tuple3<Long, Double[], Integer>> x) throws Exception {
        return new Tuple2<Long, Integer>(x._2()._1(), x._1());
      }
    }).sortByKey(true)
        .mapToPair(new PairFunction<Tuple2<Long, Integer>, String, String>() {
          public Tuple2<String, String> call(Tuple2<Long, Integer> x) throws Exception {
            String ID = x._1().toString();
            String tag = x._2().toString();
            int len = ID.length();
            String meterID = ID.substring(0, len-6);
            String YYMMDD = ID.substring(len-6);
            return new Tuple2<String, String>(meterID + "|20" + YYMMDD + "|" + tag, null);
          }
        });
  }

  protected static JavaPairRDD<String, String> entropy(final JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assigned) //assignedCentroidID, (ID, pointArray, 1)
  {
    return assigned.mapToPair(new PairFunction<Tuple2<Integer, Tuple3<Long, Double[], Integer>>, Integer, Integer>() {
      public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple3<Long, Double[], Integer>> x) throws Exception {
        String ID = x._2()._1().toString();
        int meterID = Integer.parseInt(ID.substring(0, ID.length()-6));
        return new Tuple2<Integer, Integer>(meterID, x._1());
      }
    }).groupByKey()
        .mapToPair(new PairFunction<Tuple2<Integer, Iterable<Integer>>, String, String>() {
          public Tuple2<String, String> call(Tuple2<Integer, Iterable<Integer>> x) throws Exception {
            Integer meterID = x._1();
            Map<Integer, Integer> map = new HashMap<Integer, Integer>();
            Iterator<Integer> it = x._2().iterator();
            int total = 0;
            while (it.hasNext()) {
              Integer centroidID = it.next();
              if (map.containsKey(centroidID)) {
                int count = map.get(centroidID);
                map.put(centroidID, ++count);
              } else {
                map.put(centroidID, 1);
              }
              ++total;
            }
            double entropy = 0.0;
            for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
              double prop = 1.0 * entry.getValue() / total;
              entropy += prop * Math.log(prop);
            }
            entropy *= -1.0;
            return new Tuple2<String, String>(meterID + "|" + entropy, null);
          }
        });
  }


  protected static Double[] calculateOverallCentroid(final JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assigned ){//assignedCentroidID, (ID, pointArray, 1)
    Tuple2<Integer, Tuple3<Long, Double[], Integer>> tuple = assigned.reduce(new Function2<Tuple2<Integer, Tuple3<Long, Double[], Integer>>, Tuple2<Integer, Tuple3<Long, Double[], Integer>>, Tuple2<Integer, Tuple3<Long, Double[], Integer>>>() {
      public Tuple2<Integer, Tuple3<Long, Double[], Integer>> call(Tuple2<Integer, Tuple3<Long, Double[], Integer>> x, Tuple2<Integer, Tuple3<Long, Double[], Integer>> y) throws Exception {
        Double[] newArray = new Double[x._2()._2().length];
        for (int i = 0; i < newArray.length; ++i) {
          newArray[i] = x._2()._2()[i] + y._2()._2()[i];
        }
        int n = x._2()._3() + y._2()._3();
        return new Tuple2<Integer, Tuple3<Long, Double[], Integer>>(-1, new Tuple3<Long, Double[], Integer>(-1L, newArray, n));
      }
    });

    Double[] overallCentroid = tuple._2()._2();
    int N = tuple._2()._3();
    for (int i=0; i<overallCentroid.length; ++i){
      overallCentroid[i] /= 1.0*N;
    }
    return overallCentroid;
  }


  protected static double calculateSST(final JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assigned, final Broadcast<Double[]>broadcastedOverallCentroid){
    return assigned.map(new Function<Tuple2<Integer,Tuple3<Long,Double[],Integer>>, Double>() {
      public Double call(Tuple2<Integer, Tuple3<Long, Double[], Integer>> x) throws Exception {
        Double[] overallCentroidArray = broadcastedOverallCentroid.getValue();
        Double[] point = x._2()._2();
        double dist = euclideanDistance(point, overallCentroidArray);
        return Math.pow(dist, 2);
      }
    }).reduce(new Function2<Double, Double, Double>() {
      public Double call(Double x, Double y) throws Exception {
        return x+y;
      }
    }).doubleValue();
  }


  protected static double calculateSSB(final JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assigned, //assignedCentroidID, (ID, pointArray, 1)
                                       final Map<Integer, Double[]> centroidMap,
                                       final Double[]overallCentroidArray) {

    Map<Integer, Integer> centroidCountMap = assigned.mapToPair(new PairFunction<Tuple2<Integer, Tuple3<Long, Double[], Integer>>, Integer, Integer>() {
      public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple3<Long, Double[], Integer>> x) throws Exception {
        return new Tuple2<Integer, Integer>(x._1(), 1);
      }
    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer x, Integer y) throws Exception {
        return x + y;
      }
    }).collectAsMap();

    double SSB = 0.0;
    for (Map.Entry<Integer, Double[]> entry : centroidMap.entrySet()) {
      Integer centroidID = entry.getKey();
      Double[] centroidArray = entry.getValue();
      Integer n = centroidCountMap.get(centroidID);
      Double dist = euclideanDistance(centroidArray, overallCentroidArray);
      SSB += n * Math.pow(dist, 2);
    }
    return SSB;
  }


  protected static double calculateSSW(final JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assigned, //assignedCentroidID, (ID, pointArray, 1)
                                       final Broadcast<Map<Integer, Double[]>> centroids) {
     return assigned.map(new Function<Tuple2<Integer, Tuple3<Long, Double[], Integer>>, Double>() {
      public Double call(Tuple2<Integer, Tuple3<Long, Double[], Integer>> x) throws Exception {
        Integer cetroidID = x._1();
        Double[] centroid = centroids.getValue().get(cetroidID);
        double dist = euclideanDistance(x._2()._2(), centroid);
        return Math.pow(dist, 2);
      }
    }).reduce(new Function2<Double, Double, Double>() {
      public Double call(Double x, Double y) throws Exception {
        return x + y;
      }
    }).doubleValue();
  }

  public static void run(String pointInputDir,
                         String centroidInputDir,
                         String outputDir,
                         int maxIteration,
                         double covergenceDelta,
                         int minPartitions,
                         int maxShiftNum,
                         int startClusterNum,
                         int endClusterNum,
                         double leastCoverage) {
    SparkConf conf = new SparkConf().setAppName("Liu: K-Means Clustering");
    log.setLevel(Level.INFO);

    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaPairRDD<Long, Double[]> pointsRDD = loadPoints(sc, pointInputDir, minPartitions).cache(); //(ID, pointArray)
    JavaPairRDD<Integer, Double[]> seedCentroidsRDD = loadCentroids(sc, centroidInputDir).cache();//(centroidID, pointArray)




    JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assignedPointsRDD = null; //Key=centroidID, values=(ID, pointArray, 1)
    JavaPairRDD<Integer, Double[]> centroidsRDD = null;  //(CentroidID, centroidArray)
    Map<Integer, Double[]> centroidMap = null;

    boolean hasCoverage = false;
    for (int K=startClusterNum; !hasCoverage && K<(endClusterNum+1); K+=1) {
      int iteration = 0;
      boolean hasConverged = false;
      centroidMap = initCentroids(seedCentroidsRDD, K);
      while (iteration < maxIteration && !hasConverged) {
       // log.info("Liu:  Starting Iteration " + iteration + "\n");

        Broadcast<Map<Integer, Double[]>> broadcast = sc.broadcast(centroidMap);
        Accumulator<Integer> accumulator = sc.accumulator(0);
        assignedPointsRDD = assign(broadcast, pointsRDD, maxShiftNum);

        centroidsRDD = calculateNewCentroids(assignedPointsRDD, broadcast, accumulator, covergenceDelta);

        centroidMap = centroidsRDD.collectAsMap();
        hasConverged = (0 == accumulator.value());
        ++iteration;
      }
      //log.info("Liu: Converged at Iteration " + iteration + ", hasConverged=" + hasConverged + "\n");
      Double[] overallCentroidArray = calculateOverallCentroid(assignedPointsRDD);
      long N = assignedPointsRDD.count();
      double SST = calculateSST(assignedPointsRDD, sc.broadcast(overallCentroidArray));
      double SSW = calculateSSW(assignedPointsRDD, sc.broadcast(centroidMap));
      double SSB = calculateSSB(assignedPointsRDD, centroidMap, overallCentroidArray);
      double coverage = SSB/SST;
      double CH = (SSB*(N-K))/(SSW*(K-1));
      log.info("Liu: K="+K+ " Iteration="+iteration+"  SSB="+ SSB + " SST="+SST + " Coverage=" + coverage + " SSW="+SSW + " CH=" + CH);
      hasCoverage = coverage>=leastCoverage;
    }

    List<String> list = new ArrayList<String>();
    for (Map.Entry<Integer, Double[]> centroid : centroidMap.entrySet()) {
      Integer label = centroid.getKey();
      Double[] point = centroid.getValue();
      list.add(label + "|" + StringUtils.join(point, pointDelim));
    }

    sc.parallelize(list)
        .saveAsTextFile(outputDir + "/centroid");

    tagPoints(assignedPointsRDD)
        .saveAsNewAPIHadoopFile(outputDir + "/taggedpoints", Text.class, NullWritable.class, TextOutputFormat.class);

    entropy(assignedPointsRDD)
        .saveAsNewAPIHadoopFile(outputDir + "/entropy", Text.class, NullWritable.class, TextOutputFormat.class);
    sc.close();
  }

  public static void main(String[] args) {
    String usage = "java ca.uwaterloo.iss4e.clustering.KSCClustering pointInputDir centroidInputDir outputDir maxIteration covergenceDelta minPartitions maxShiftNum startClusterNum endClusterNum leastCoverage";
    if (args.length != 10) {
      log.error(usage);
    } else {
      KSCClustering.run(
          args[0], //PointInputDir
          args[1], //centroidInputDir
          args[2], //outputDir
          Integer.parseInt(args[3]),
          Double.parseDouble(args[4]),
          Integer.parseInt(args[5]),
          Integer.parseInt(args[6]),
          Integer.parseInt(args[7]), //startClusterNum
          Integer.parseInt(args[8]), //endClusterNum
          Double.parseDouble(args[9]) //leastCoverage
      );
    }
  }
}
