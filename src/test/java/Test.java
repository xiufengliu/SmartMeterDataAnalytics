/**
 * Created by xiuli on 5/31/16.
 */
public class Test {
  public static void main(String[]args){
    //essex: 19419|2011-04-03 04:00:00|1.21|1.6
    //dataport: 871|2013-12-02 12:00:00-06|0.22178333333333333333
    //water: 1|2012-01-01 12:00:00|
    //water String line ="1|2012-01-01 19:00:00|0.4";
    String line ="19419|2011-04-03 04:00:00|1.21|1.6";
    String[] fields = line.split("\\|");
    String meterID = fields[0];
    String YYMMDDHH = ((fields[1].replace("-", "").replace(" ", "").split(":"))[0]).substring(2);
    System.out.println(fields.length);
  }
}
