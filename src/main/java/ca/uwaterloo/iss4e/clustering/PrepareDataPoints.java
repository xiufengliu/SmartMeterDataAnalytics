package ca.uwaterloo.iss4e.clustering;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import scala.Tuple2;

/**
 * Created by xiuli on 6/1/16.
 */
public class PrepareDataPoints {
  private static final Logger log = Logger.getLogger(PrepareDataPoints.class);
  //private static final String fieldDelim = "\\t";

  public static boolean isNumeric(String str)
  {
    return str.matches("-?\\d+(.\\d+)?");
  }

  public static void preparePoints(final String inputDir, final String outputDir, final int windowSize) {
    SparkConf conf = new SparkConf().setAppName("Liu: Prepare data points");

    //1000|2010-11-13 22:00:00|1.224
    //1000|2010-11-13 23:00:00|0.551
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.textFile(inputDir)
        .flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<Integer, Double>>() {
          public Iterable<Tuple2<String, Tuple2<Integer, Double>>> call(String s) throws Exception {
            List<Tuple2<String, Tuple2<Integer, Double>>> list = new ArrayList<Tuple2<String, Tuple2<Integer, Double>>>();
            String[] fields = s.split("\t");
            if (fields.length>2) {
              String meterID = fields[0];
              String timestamp = fields[1];
              String readingStr = fields[2];
              if (StringUtils.isNotEmpty(readingStr) && PrepareDataPoints.isNumeric(readingStr)) {
                Double reading = Double.parseDouble(readingStr);
                String[] arr = timestamp.replace("-", "").split(" ");
                String ID = meterID + arr[0].substring(2); //1000 120101
                Integer hour = Integer.parseInt(arr[1].split(":")[0]);
                list.add(new Tuple2<String, Tuple2<Integer, Double>>(ID, new Tuple2<Integer, Double>(hour, reading)));
              }
            }
            return list;
          }
        }).groupByKey()
        .flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<Integer, Double>>>, String, Tuple2<String, Double[]>>() {
          public Iterable<Tuple2<String, Tuple2<String, Double[]>>> call(Tuple2<String, Iterable<Tuple2<Integer, Double>>> x) throws Exception {
            String ID = x._1();
            String meterID = ID.substring(0, ID.length() - 6);
            String YYMMDD = ID.substring(ID.length() - 6);

            Iterator<Tuple2<Integer, Double>> itr = x._2().iterator();
            List<Tuple2<String, Tuple2<String, Double[]>>> list = new ArrayList<Tuple2<String, Tuple2<String, Double[]>>>();
            int cnt = 0;
            Double[] readings = new Double[24];
            double sum = 0.0;
           // int zeros = 0;
            //int ones = 0;
            while (itr.hasNext()) {
              Tuple2<Integer, Double> t = itr.next();
              int h = t._1();
              readings[h] = t._2();
              ++cnt;
              //zeros += readings[h]==0.0?1:0;
              //ones += readings[h]==1.0?1:0;
              sum += t._2().doubleValue();
            }
            //boolean isSparse = (zeros==23)&&(ones==1);
            if (cnt==24 && sum>0.0) {
              list.add(new Tuple2<String, Tuple2<String, Double[]>>(meterID, new Tuple2<String, Double[]>(YYMMDD, readings)));
            }
            return list;
          }
        }).groupByKey()
        .flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Double[]>>>, String, String>() {
          public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<Tuple2<String, Double[]>>> x) throws Exception {
            String meterID = x._1();
            Iterator<Tuple2<String, Double[]>> itr = x._2().iterator(); // (YYMMDD, readingArray)
            TreeMap<String, Double[]> sortedMap = new TreeMap<String, Double[]>();
            while (itr.hasNext()) {
              Tuple2<String, Double[]> t = itr.next();
              sortedMap.put(t._1(), t._2());
            }

            Collection<String> YYMMDDs = sortedMap.keySet();
            String[] YYMMDDArray = YYMMDDs.toArray(new String[YYMMDDs.size()]);

            List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
            if (windowSize == 24) {
              for (int i = 0; i < YYMMDDArray.length; ++i) {
                String YYMMDD = YYMMDDArray[i];
                Double[] dailyReadings = sortedMap.get(YYMMDD);
                String dailyReadingStr = StringUtils.join(dailyReadings, ",");
                ret.add(new Tuple2<String, String>(meterID + YYMMDD + "|" + dailyReadingStr, null));
              }
            } else {
              Double[] readings = new Double[windowSize];
              for (int i = 1; i < YYMMDDArray.length - 1; ++i) {
                int n = (windowSize - 24) / 2;
                Double[] pre = sortedMap.get(YYMMDDArray[i - 1]);
                Double[] cur = sortedMap.get(YYMMDDArray[i]);
                Double[] next = sortedMap.get(YYMMDDArray[i + 1]);
                System.arraycopy(pre, 24 - n, readings, 0, n);
                System.arraycopy(cur, 0, readings, n, 24);
                System.arraycopy(next, 0, readings, n + 24, n);
                String dailyReadingStr = StringUtils.join(readings, ",");
                ret.add(new Tuple2<String, String>(meterID + YYMMDDArray[i] + "|" + dailyReadingStr, null));
              }
            }
            return ret;
          }
        }).sortByKey(true)
        .saveAsNewAPIHadoopFile(outputDir, NullWritable.class, Text.class, TextOutputFormat.class);
    sc.close();
  }

  public static void prepareCentroids(final String inputDir, final String outputDir, final int windowSize) {
    //8674350130131|0.533,0.436,0.45,0.463,0.458,1.088,0.617,0.549,0.261,0.22,1.188,0.257,0.23,0.246,0.246,0.278,0.36,0.404,0.538,0.665,0.717,0.813,0.62,0.376,0.352,0.353
    SparkConf conf = new SparkConf().setAppName("Liu: Prepare centroids");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Double[]> centroids = sc.textFile(inputDir)
        .map(new Function<String, Double[]>() {
          public Double[] call(String s) throws Exception {
            String[] points = (s.split("\\|")[1]).split(",");
            int start = (windowSize - 24) / 2;
            double sum = 0;
            Double[] pointArray = new Double[24];
            for (int i = start; i < start + 24; ++i) {
              pointArray[i - start] = Double.parseDouble(points[i]);
              sum += pointArray[i - start];
            }
            for (int i = 0; i < 24; ++i) {
              pointArray[i] = pointArray[i] / sum;
            }
            return pointArray;
          }
        }).takeSample(false, 100);

    List<String> centroidList = new ArrayList<String>();
    for (int i = 0; i < centroids.size(); ++i) {
      StringBuffer buf = new StringBuffer();
      Double[] cents = centroids.get(i);
      buf.append(i + 1).append("|");
      for (int j = 0; j < cents.length; ++j) {
        buf.append(cents[j]);
        if (j != cents.length - 1) {
          buf.append(",");
        }
      }
      centroidList.add(buf.toString());
    }

    sc.parallelize(centroidList, 1).mapToPair(new PairFunction<String, String, String>() {
      public Tuple2<String, String> call(String s) throws Exception {
        return new Tuple2<String, String>(null, s);
      }
    }).saveAsNewAPIHadoopFile(outputDir, NullWritable.class, Text.class, TextOutputFormat.class);
    sc.close();
  }

  public static void main(String[] args) {
    String usage = "java ca.uwaterloo.iss4e.clustering.PrepareDataPoints srcInputDir pointOutputDir centroidOutputDir windowSize";
    if (args.length != 4) {
      log.error(usage);
    } else {
      PrepareDataPoints.preparePoints(args[0], args[1], Integer.parseInt(args[3]));
      PrepareDataPoints.prepareCentroids(args[1], args[2], Integer.parseInt(args[3]));
    }
  }
}
