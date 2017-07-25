package com.gordonzu;

import java.util.Arrays;
import scala.Tuple2;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {
	public static void main(String[] args) throws Exception {
    	String inputFile = args[0];
    	String outputFile = args[1];

    	SparkConf conf = new SparkConf().setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

    	JavaRDD<String> rdd = sc.textFile(inputFile);

	    JavaPairRDD<String, Integer> counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")))
                                                 .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                                                 .reduceByKey((x, y) -> x + y);
        
        counts.saveAsTextFile(outputFile);
	}
}

        /*
        JavaRDD<String> words = rdd.flatMap(x -> Arrays.asList(x.split(" ")));
        JavaPairRDD<String, Integer> counts = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                                                   .reduceByKey((x, y) -> x + y);
        */

        /*
    	JavaRDD<String> words = input.flatMap(
    		new FlatMapFunction<String, String>() {
        		public Iterable<String> call(String x) {
          		    return Arrays.asList(x.split(" "));
        }}); 

        JavaPairRDD<String, Integer> counts = words.mapToPair(
      		new PairFunction<String, String, Integer>(){
        		public Tuple2<String, Integer> call(String x){
          		return new Tuple2(x, 1);
        		}}).reduceByKey(new Function2<Integer, Integer, Integer>(){
            	public Integer call(Integer x, Integer y){ return x + y;}});
        */



