package pitt.cloud.Access1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Accesspart2
{
    public static void pathfinder( String filename )
    {
        SparkConf conf = new SparkConf().setMaster("yarn").setAppName("AccessLog3");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile( "hdfs:///user/root/accesslog/access_log" ).repartition(2);

        JavaRDD<String> words = input.flatMap( s -> Arrays.asList( getPath(s) ).iterator() );

        JavaPairRDD<String, Integer> counts = words.mapToPair( t -> new Tuple2<>( t, 1 ) ).reduceByKey( (x, y) -> (int)x + (int)y ).repartition(2);
        	
        JavaPairRDD<Integer, String> swap = counts.mapToPair(item -> item.swap());
        
        JavaPairRDD<Integer, String> sort = swap.sortByKey(false).repartition(2);
               
        JavaPairRDD<Integer, String> filter = sort.filter(action-> action.equals("/assets/js/lowpro.js") ||action.equals("/favicon.ico"));
        
        filter.foreach(path -> System.out.println(path._2+ " "+path._1));
        
        //foreach.saveAsTextFile( "hdfs:///user/root/output3" );
    }
    
    public static String getPath(String line){
    	String Line = new String(line);
    	//String item = null;
       // String lp = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)";
             Pattern logPattern = Pattern//.compile(lp);//private static 
             .compile("([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\]"
                    + " \"([^\"]*)\""
                     + " ([^ ]*) ([^ ]*).*");
            //.compile("%h %l %u %t \"%r\" %>s %b");
             
         Matcher matcher = logPattern.matcher(Line);
         if (matcher.matches()) {
        	 String item = matcher.group(5);
             String path = item.split(" ")[1];
             return path;
             }
         return "";
    }

    public static void main( String[] args )
    {	
    	long startTime = System.currentTimeMillis();
        pathfinder( "" );
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        long totalTime2 = totalTime/1000;
        System.out.println("Timetaken: " + totalTime2);
    }
}	