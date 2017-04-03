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
import java.io.Serializable;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


public class Task2 {

	    public static void top20( String filename )
	    {
	        SparkConf conf = new SparkConf().setMaster("yarn").setAppName("top10");
	       	        
	        SparkSession part = SparkSession.builder().config(conf).getOrCreate();
	        
	        Dataset<Row> artist = part.read().format("com.databricks.spark.csv").option("delimiter", "\t").option("inferSchema", "true")
	        		.option("header", "true").load("hdfs:///user/root/accesslog/user_artists.dat");

	     Dataset<Row>  colu = artist.groupBy(artist.col("artistID")).sum("weight");
	     Dataset<Row> top = colu.sort(colu.col("sum(weight)").desc());
	     top.show(10,false);
	      
	    }

	    public static void main( String[] args )
	    {
	         top20( "" );
	    }
	}		
