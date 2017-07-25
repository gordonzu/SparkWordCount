package com.gordonzu;


import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class SparkWordCount {

    public static void main(String[] args) throws Exception {

        System.out.println(System.getProperty("hadoop.home.dir"));

        String inputPath = args[0];
        String outputPath = args[1];

        FileUtils.deleteQuietly(new File(outputPath));

        SparkConf conf = new SparkConf().setAppName("word-counter").setMaster("local").set("spark.cores.max", "10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(inputPath);

        JavaPairRDD<String, Integer> counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")))
                .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> finalCounts = counts.filter((x) -> x._1().contains("@"))
                .collect();

        for(Tuple2<String, Integer> count: finalCounts)
                System.out.println(count._1() + " " + count._2());

         counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")))
                 .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                 .reduceByKey((x, y) -> x + y);

        counts = counts.filter((x) -> x._2() > 20);

        long time = System.currentTimeMillis();
        long countEntries = counts.count();
        System.out.println(countEntries + ": " + String.valueOf(System.currentTimeMillis() - time));

       counts.saveAsTextFile(outputPath);
        sc.close();

    }
}
