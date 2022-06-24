package cn.edu.ecnu.spark.example.java.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import javax.swing.*;
import java.util.Arrays;
import java.util.Iterator;

public class WordCount_Combine {
    public static void run(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCountJava_Combine");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String,Integer>(word,1);
                    }
                }
        );

        JavaPairRDD<String,Integer> wordCounts = pairs.combineByKey(
                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer) throws Exception {
                        return integer;
                    }
                },
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                },
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                }
        );

        wordCounts.saveAsTextFile(args[1]);

        sc.stop();
    }

    public static void main(String[] args){
        run(args);
    }
}
