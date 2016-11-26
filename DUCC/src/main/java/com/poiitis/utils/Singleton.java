package com.poiitis.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author Poiitis Marinos
 */
public final class Singleton {
    private static JavaSparkContext instance = null;
    private Singleton() {}
    
    public static synchronized JavaSparkContext getSparkContext() {
        if (instance == null){
            SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
            instance = new JavaSparkContext(conf);
        }
        return instance;
    }
}
