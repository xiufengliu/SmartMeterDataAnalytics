package ca.uwaterloo.iss4e.clustering;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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
  private static final String fieldDelim = "\\|";
  private static final String pointDelim = ",";

  public static void run(final String inputDir, final String outputDir, final int windowSize) {
    SparkConf conf = new SparkConf().setAppName("Liu: Prepare data points");

    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.textFile(inputDir)
        .mapToPair(new PairFunction<String, String, Double[]>() {
          public Tuple2<String, Double[]> call(String line) throws Exception {//line =
            //essex: 19419|2011-04-03 04:00:00|1.21|1.6
            //dataport: 871|2013-12-02 12:00:00-06|0.22178333333333333333
            //water: 1|2012-01-01 12:00:00|
            if (line.endsWith("|")){
              line = line+"-1.0";
            }
            String[] fields = line.split(fieldDelim);
            String meterID = fields[0];
            String YYMMDDHH = ((fields[1].replace("-", "").replace(" ", "").split(":"))[0]).substring(2);
            return new Tuple2<String, Double[]>(meterID, new Double[]{Double.parseDouble(YYMMDDHH), Double.parseDouble(fields[2])});
          }
        }).groupByKey()
        .flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Double[]>>, Integer, String>() {
          public Iterable<Tuple2<Integer, String>> call(Tuple2<String, Iterable<Double[]>> tuple) throws Exception {// (MeterID, Iterable[YYMMDDHH, reading])
            String meterID = tuple._1();
            Iterator<Double[]> itr = tuple._2().iterator();
            TreeMap<Integer, Double> sortedMap = new TreeMap<Integer, Double>();

            while (itr.hasNext()) {
              Double[] values = itr.next();
              int YYMMDDHH = (int)values[0].doubleValue();
              sortedMap.put(YYMMDDHH, values[1]);
            }



            Collection<Integer> YYMMDDHHs = sortedMap.keySet();
            Integer[] YYMMDDHHArray = YYMMDDHHs.toArray(new Integer[YYMMDDHHs.size()]);

            Double[] point = new Double[windowSize];
            List<Tuple2<Integer, String>> points = new ArrayList<Tuple2<Integer, String>>();

            int shift = (windowSize - 24) / 2;

            int startPost = 0;
            for (;startPost<YYMMDDHHArray.length; ++startPost){//This ensure start from 00:00:00
              if (YYMMDDHHArray[startPost]%100==0){
                break;
              }
            }

            startPost += (shift == 0) ? 0 : (24 - shift);
            for (int i = startPost; i < YYMMDDHHArray.length - windowSize; i += 24) {
              double sum = 0.0;
              boolean isValid = true;
              for (int j = i + 0; j < i + windowSize; ++j) {
                Integer YYMMDDHH = YYMMDDHHArray[j];
                Double reading = sortedMap.get(YYMMDDHH);
                if (reading < 0.0) {
                  isValid = false;
                  break;
                }
                point[j - i] = reading;
                sum += reading.doubleValue();
              }

              if (isValid && (sum > 0.0)) {
                int YYMMDDHH = (int) YYMMDDHHArray[i + shift].doubleValue();
                String YYMMDD = String.valueOf(YYMMDDHH).substring(0, 6);
                StringBuffer buf = new StringBuffer();
                buf.append(meterID).append(YYMMDD).append("|");
                for (int j = 0; j < windowSize; ++j) {
                 // buf.append(point[j] / sum); // only for making centroid
                  buf.append(point[j]);
                  if (j < windowSize - 1) {
                    buf.append(pointDelim);
                  }
                }
                points.add(new Tuple2<Integer, String>(null, buf.toString()));
              }
            } // end For i
            return points;
          }
        })
        .saveAsNewAPIHadoopFile(outputDir, NullWritable.class, Text.class, TextOutputFormat.class);

    sc.close();
  }

  public static void main(String[] args) {
    String usage = "java ca.uwaterloo.iss4e.clustering.PrepareDataPoints srcInputDir pointOutputDir windowSize";
    if (args.length != 3) {
      log.error(usage);
    } else {
      PrepareDataPoints.run(args[0],
          args[1],
          Integer.parseInt(args[2])
      );
    }
  }
}
