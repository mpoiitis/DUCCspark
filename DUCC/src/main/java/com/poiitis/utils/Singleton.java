package com.poiitis.utils;

import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author Poiitis Marinos
 */
public final class Singleton {
    private static JavaSparkContext instance;
    private Singleton() {}
    
    public static synchronized void setSparkContext(JavaSparkContext sc){
        instance = sc;
    }
    public static synchronized JavaSparkContext getSparkContext() {
        return instance;
    }
    
    public static synchronized void closeSparkContext(){
        instance.close();
    }
}
