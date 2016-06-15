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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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


  protected static JavaPairRDD<Integer, Tuple2<Integer,Double[]>> loadCentroids(JavaSparkContext sc, String centroidInputDir) {
    return sc.textFile(centroidInputDir)
        .mapToPair(new PairFunction<String, Integer, Tuple2<Integer,Double[]>>() {
          public Tuple2<Integer, Tuple2<Integer,Double[]>> call(String line) throws Exception {
            String[] fields = line.split(keyDelim);
            int centroidID = Integer.parseInt(fields[0]);
            String[] points = fields[1].split(pointDelim);
            Double[] pointArray = new Double[points.length];
            for (int i = 0; i < points.length; ++i) {
              pointArray[i] = Double.parseDouble(points[i]);
            }
            return new Tuple2<Integer, Tuple2<Integer,Double[]>>(centroidID, new Tuple2<Integer, Double[]>(1, pointArray));// (centroidID, (NumberOfPointMadeThisCentroid, centroidArray))
          }
        });
  }

  /**
   *
   * @param allCentroids  (centroidID, (NumberOfPointMadeThisCentroid, centroidArray))
   * @param K Number of Centroids
   * @return
   */
  protected static Map<Integer, Tuple2<Integer,Double[]>> initCentroids(JavaPairRDD<Integer, Tuple2<Integer,Double[]>> allCentroids, final int K) {
    List<Tuple2<Integer, Tuple2<Integer, Double[]>>> list = allCentroids.takeSample(false, K);
    Map<Integer, Tuple2<Integer,Double[]>> map = new HashMap<Integer, Tuple2<Integer, Double[]>>();
    int i = 1;
    for (Tuple2<Integer, Tuple2<Integer, Double[]>> e: list){
      map.put(i++, e._2());
    }
    return map;
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

  /**
   *
   * @param point  is not normalized
   * @param centroid  is normalized.
   * @param maxShiftNum
   * @return (minimal distance, normalized shifted array)
   */
  protected static Tuple2<Double, Double[]> KscDistance(final Double[] point, final Double[] centroid, int maxShiftNum) {//
    double minDist = Double.MAX_VALUE;
    Double[] minChoppedArray = null;
    for (int shift = -maxShiftNum; shift < maxShiftNum + 1; ++shift) {
      Double[] shiftedArray = rotateArray(point, shift); //Will make a new copy
      Double[] choppedArray = chopEdges(shiftedArray, centroid.length);
      normalize(choppedArray);
      double dist = euclideanDistance(choppedArray, centroid);
      //double dist = cosineSimilarity(choppedArray, centroid);
      if (dist < minDist) {
        minDist = dist;
        minChoppedArray = choppedArray;
      }
    }
    return new Tuple2<Double, Double[]>(minDist, minChoppedArray);
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

  /**
   *
   * @param centroids (CentroidID, (NumberofElements, CentroidArray))
   * @param points (ID, pointArray)
   * @param maxShiftNum
   * @return (CentroidID, (ID, pointArray, 1))
   */
  protected static JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assign(final Broadcast<Map<Integer, Tuple2<Integer,Double[]>>> centroids,
                                                                                final JavaPairRDD<Long, Double[]> points,
                                                                                final int maxShiftNum) {
    return points.mapToPair(new PairFunction<Tuple2<Long, Double[]>, Integer, Tuple3<Long, Double[], Integer>>() {
      public Tuple2<Integer, Tuple3<Long, Double[], Integer>> call(Tuple2<Long, Double[]> point) throws Exception {//(ID, pointArray)
        double minDist = Double.MAX_VALUE;
        int centroidID = -1;
        Double[] shiftedArray = null;

        for (Map.Entry<Integer, Tuple2<Integer,Double[]>> entry : centroids.getValue().entrySet()) {
          Double[] centroid = entry.getValue()._2();
          Tuple2<Double, Double[]> distArray = KscDistance(point._2(), centroid, maxShiftNum);
          if (distArray._1() < minDist) {
            minDist = distArray._1();
            centroidID = entry.getKey();
            shiftedArray = distArray._2();
          }
        }
        return new Tuple2<Integer, Tuple3<Long, Double[], Integer>>(centroidID, new Tuple3<Long, Double[], Integer>(point._1(), shiftedArray, 1)); //(ID, pointArray, 1)
      }
    });
  }

  /**
   *
   * @param assigned (centroidID, (ID, shiftedArray, 1))
   * @param oldCentroids
   * @param converged
   * @param delta
   * @return (centroidID, Tuple2(NumberOfElements, centroidArray))
   */
  protected static JavaPairRDD<Integer, Tuple2<Integer,Double[]>> calculateNewCentroids(final JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assigned,
                                                                        final Broadcast<Map<Integer, Tuple2<Integer,Double[]>>> oldCentroids,
                                                                        final Accumulator<Integer> converged,
                                                                                        final Accumulator<Double>                deltaAccumulator,
                                                                                        final double delta) {
    /*int numCentroids = oldCentroids.getValue().size();
    CentroidPartitioner partition = new CentroidPartitioner(numCentroids);*/

    return assigned.reduceByKey(new Function2<Tuple3<Long, Double[], Integer>,
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
    }).mapToPair(new PairFunction<Tuple2<Integer, Tuple3<Long, Double[], Integer>>, Integer, Tuple2<Integer,Double[]>>() {
      public Tuple2<Integer, Tuple2<Integer,Double[]>> call(Tuple2<Integer, Tuple3<Long, Double[], Integer>> x) throws Exception {
        Integer centroidID = x._1();
        Double[] partial = x._2()._2();
        int N = x._2()._3();
        Double[] newCentroid = new Double[partial.length];
        for (int i = 0; i < partial.length; ++i) {
          newCentroid[i] = partial[i] / N;
        }
        Double[] oldCentroid = oldCentroids.getValue().get(centroidID)._2();
        double dist = euclideanDistance(newCentroid, oldCentroid);
        if (dist > delta) {
          converged.add(1);
          deltaAccumulator.add(dist);
        }
        return new Tuple2<Integer, Tuple2<Integer,Double[]>>(centroidID, new Tuple2<Integer, Double[]>(N, newCentroid));
      }
    });
  }

  /**
   *
   * @param assigned =(assignedCentroidID, (ID, pointArray, 1))
   * @return
   */
  protected static JavaPairRDD<String, String> tagPoints(final JavaPairRDD<Long, Double[]> original, final JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assigned)
  {
    return assigned.mapToPair(new PairFunction<Tuple2<Integer, Tuple3<Long, Double[], Integer>>, Long,  Tuple2<Integer, Double[]>>() {
      public Tuple2<Long,  Tuple2<Integer, Double[]>> call(Tuple2<Integer, Tuple3<Long, Double[], Integer>> x) throws Exception {
        return new Tuple2<Long, Tuple2<Integer, Double[]>>(x._2()._1(), new Tuple2<Integer, Double[]>(x._1(), x._2()._2())); //(ID, (CentroidID, centroidArray))
      }
    }).cogroup(original)
      .flatMapToPair(new PairFlatMapFunction<Tuple2<Long,Tuple2<Iterable<Tuple2<Integer,Double[]>>,Iterable<Double[]>>>, String, String>() {
      public Iterable<Tuple2<String, String>> call(Tuple2<Long, Tuple2<Iterable<Tuple2<Integer, Double[]>>, Iterable<Double[]>>> x) throws Exception {
        /*String ID = x._1().toString();
        int len = ID.length();
        String meterID = ID.substring(0, len-6);
        String YYMMDD = ID.substring(len-6);*/

        Iterator<Tuple2<Integer, Double[]>> centItr = x._2()._1().iterator(); //(CentroidID, centroidArray)
        Iterator<Double[]> oriIt = x._2()._2().iterator();

        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
        if (centItr.hasNext() && oriIt.hasNext()){
          Tuple2<Integer, Double[]> cent = centItr.next();
          Integer centroidID = cent._1();
          Double[]normalizedShiftedCentroid = cent._2();
          Double[] ori = oriIt.next();
          int shift = (ori.length-normalizedShiftedCentroid.length)/2;

          Double[] newOri = new Double[normalizedShiftedCentroid.length];
          double sum = 0.0;
          for (int i=0; i<normalizedShiftedCentroid.length; ++i){
            newOri[i] = ori[i+shift];
            sum += newOri[i].doubleValue();
          }

          for (int i=0; i<newOri.length; ++i){
            newOri[i] /= sum;
          }
          list.add(new Tuple2<String, String>(centroidID+"|"+StringUtils.join(newOri, ",")+"|"+StringUtils.join(normalizedShiftedCentroid, ","), null));
        }
        return list;
      }
    }).sortByKey(true);


        /*.sortByKey(true)
        .mapToPair(new PairFunction<Tuple2<Long,  Tuple2<Integer, Double[]>>, String, String>() {
          public Tuple2<String, String> call(Tuple2<Long,  Tuple2<Integer, Double[]>> x) throws Exception {
            String ID = x._1().toString();
            String centroidID = x._2()._1().toString();
            Double[] centroidArray = x._2()._2();
            int len = ID.length();
            String meterID = ID.substring(0, len-6);
            String YYMMDD = ID.substring(len-6);
            return new Tuple2<String, String>(meterID + "|20" + YYMMDD + "|" + centroidID+"|"+StringUtils.join(centroidArray,","), null);
          }
        });*/
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

  /**
   *
   * @param centroidMap (CentroidID, (NumberOfElements, CentroidArray))
   * @return (TotalNumberOfPoints, overall Centroid)
   */

  protected static Tuple2<Integer, Double[]> calculateOverallCentroid(final Map<Integer, Tuple2<Integer,Double[]>> centroidMap ){
    int N = 0;
    double[] overallCentroid = new double[24];
    for (Map.Entry<Integer, Tuple2<Integer, Double[]>> entry : centroidMap.entrySet()){
      Tuple2<Integer, Double[]> tuple = entry.getValue();
      int n =  tuple._1();
      Double[] centroidArray = tuple._2();
      for (int i=0; i<24; ++i){
        overallCentroid[i] += centroidArray[i].doubleValue()*n;
      }
      N += n;
    }
    for (int i=0; i<24; ++i){
      overallCentroid[i] = overallCentroid[i]/N;
    }
    Double[] newOverArray = new Double[overallCentroid.length];
    for (int i=0; i<overallCentroid.length; ++i){
      newOverArray[i] = Double.valueOf(overallCentroid[i]);
    }
    return new Tuple2<Integer, Double[]>(N, newOverArray);
  }

  /**
   *
   * @param assigned (CentroidID, (ID, pointArray, 1))
   * @param broadcastedOverallCentroid overallCentroidArray
   * @return
   */
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

  /**
   *
   * @param centroidMap (CentroidID, (NumberOfElemenets, CentroidArray))
   * @param overallCentroidArray
   * @return
   */
  protected static double calculateSSB(final Map<Integer, Tuple2<Integer,Double[]>> centroidMap,
                                       final Double[]overallCentroidArray) {
    double SSB = 0.0;
    for (Map.Entry<Integer, Tuple2<Integer,Double[]>> entry : centroidMap.entrySet()) {
      Integer n = entry.getValue()._1();
      Double[] centroidArray = entry.getValue()._2();
      Double dist = euclideanDistance(centroidArray, overallCentroidArray);
      SSB += n * Math.pow(dist, 2);
    }
    return SSB;
  }


  protected static double calculateSSW(final JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assigned, //assignedCentroidID, (ID, pointArray, 1)
                                       final Broadcast<Map<Integer, Tuple2<Integer,Double[]>>> centroids) {
     return assigned.map(new Function<Tuple2<Integer, Tuple3<Long, Double[], Integer>>, Double>() {
      public Double call(Tuple2<Integer, Tuple3<Long, Double[], Integer>> x) throws Exception {
        Integer centroidID = x._1();
        Double[] centroid = centroids.getValue().get(centroidID)._2();
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
    JavaPairRDD<Integer, Tuple2<Integer,Double[]>> seedCentroidsRDD = loadCentroids(sc, centroidInputDir).cache();//(centroidID, pointArray)




    JavaPairRDD<Integer, Tuple3<Long, Double[], Integer>> assignedPointsRDD = null; //Key=centroidID, values=(ID, pointArray, 1)
    JavaPairRDD<Integer, Tuple2<Integer,Double[]>> centroidsRDD = null;  //([CentroidID,numberOfElement] centroidArray)
    Map<Integer, Tuple2<Integer,Double[]>> centroidMap = null;

    boolean hasCoverage = false;
    for (int K=startClusterNum; !hasCoverage && K<(endClusterNum+1); K+=1) {
      int iteration = 0;
      boolean hasConverged = false;
      centroidMap = initCentroids(seedCentroidsRDD, K);
      while (iteration < maxIteration && !hasConverged) {

        Broadcast<Map<Integer, Tuple2<Integer,Double[]>>> broadcastCentroids = sc.broadcast(centroidMap);
        Accumulator<Integer> accumulator = sc.accumulator(0);
        Accumulator<Double> deltaAccumulator = sc.accumulator(0.0);
        assignedPointsRDD = assign(broadcastCentroids, pointsRDD, maxShiftNum);
        centroidsRDD = calculateNewCentroids(assignedPointsRDD, broadcastCentroids, accumulator, deltaAccumulator, covergenceDelta);
        centroidMap = centroidsRDD.collectAsMap();
        hasConverged = (0 == accumulator.value());
        log.info("afancy: K="+K+" Iteration " + iteration + " dist="+deltaAccumulator.value().doubleValue());
        ++iteration;
      }
      //log.info("Liu: Converged at Iteration " + iteration + ", hasConverged=" + hasConverged + "\n");
      Tuple2<Integer, Double[]> overallCentroid = calculateOverallCentroid(centroidMap);
      long N = overallCentroid._1();
      Double[] centroidArray = overallCentroid._2();
      double SST = calculateSST(assignedPointsRDD, sc.broadcast(centroidArray));
      double SSW = calculateSSW(assignedPointsRDD, sc.broadcast(centroidMap));
      double SSB = calculateSSB(centroidMap, centroidArray);
      double coverage = SSB/SST;
      double CH = (SSB*(N-K))/(SSW*(K-1));
      log.info("Liu: K="+K+ " Iteration="+iteration+"  SSB="+ SSB + " SST="+SST + " Coverage=" + coverage + " SSW="+SSW + " CH=" + CH);
      hasCoverage = coverage>=leastCoverage;
    }

    TreeMap<Integer, Tuple2<Integer,Double[]>> treeMap = new TreeMap<Integer, Tuple2<Integer, Double[]>>();
    treeMap.putAll(centroidMap);
    long totalCount = 0L;
    for (Map.Entry<Integer, Tuple2<Integer,Double[]>> centroid : treeMap.entrySet()) {
      totalCount += centroid.getValue()._1();
    }
    List<String> list = new ArrayList<String>();
    for (Map.Entry<Integer, Tuple2<Integer,Double[]>> centroid : treeMap.entrySet()) {
      Integer centroidID = centroid.getKey();
      double share = centroid.getValue()._1()*100.0/totalCount;
      Double[] point = centroid.getValue()._2();
      list.add(centroidID + "|"+ share+ "|"+ StringUtils.join(point, pointDelim));
    }

    sc.parallelize(list)
        .saveAsTextFile(outputDir + "/centroid");

    tagPoints(pointsRDD, assignedPointsRDD)
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
          Integer.parseInt(args[3]), //maxIteration
          Double.parseDouble(args[4]), //covergenceDelta
          Integer.parseInt(args[5]), //minPartitions
          Integer.parseInt(args[6]), //maxShiftNum
          Integer.parseInt(args[7]), //startClusterNum
          Integer.parseInt(args[8]), //endClusterNum
          Double.parseDouble(args[9]) //leastCoverage
      );
    }
  }
}
