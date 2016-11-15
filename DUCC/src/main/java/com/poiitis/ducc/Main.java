package com.poiitis.ducc;

import com.poiitis.pli.PLIBuilder;
import com.poiitis.pli.PositionListIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;


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
        
        LongAccumulator lineAcc = sc.sc().longAccumulator(); 
        JavaRDD<Adult> adults = parser.parseFile(lineAcc);//TODO persist rdd without error on stdout
        
        PLIBuilder pliBuilder = new PLIBuilder(adults, true);
        pliBuilder.createInitialPLIs();
        JavaRDD<PositionListIndex> plis = pliBuilder.getPLIList();
        DuccAlgorithm duccAlgorithm = new DuccAlgorithm(parser.getColumnNames());
    }
    
}
