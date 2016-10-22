package com.poiitis.ducc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author Poiitis Marinos
 */
public class Main {
    
    private static final String INPUT_PATH = "hdfs://localhost:9000/user/marinos/input";
    
    
    public static void main(String[] args){
        
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(INPUT_PATH);
        
        Parser parser = new Parser(input);
        
        JavaRDD<Adult> adults = parser.parseFile();//TODO persist rdd without error on stdout
    }
    
}
