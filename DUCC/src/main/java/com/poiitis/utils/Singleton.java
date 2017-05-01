package com.poiitis.utils;

import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author Poiitis Marinos
 */
public class Singleton implements Serializable
{

	private static final JavaSparkContext CONTEXT;

	static
	{
		//define spark context
		SparkConf conf = new SparkConf().setAppName("Quasi Identifier Finder");
		CONTEXT = new JavaSparkContext(conf);
	}

	private Singleton(){}

	public static JavaSparkContext getSparkContext()
	{
		return CONTEXT;
	}

}
