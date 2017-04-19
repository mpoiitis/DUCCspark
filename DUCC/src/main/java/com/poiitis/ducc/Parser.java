package com.poiitis.ducc;

import com.google.common.collect.ImmutableList;
import com.poiitis.utils.Singleton;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *
 * @author Poiitis Marinos
 */
public class Parser implements Serializable{

    private static final long serialVersionUID = -3808214153694185619L;
    
    private JavaRDD<String> input;
    private ArrayList<Tuple2<String,Integer>> columnNames;
    
    public Parser(JavaRDD<String> input){    
        this.input = input;
        columnNames = new ArrayList<>();
        
        columnNames.add(new Tuple2("Age", 0));
        columnNames.add(new Tuple2("Workclass", 1));
        columnNames.add(new Tuple2("Fnlwgt", 2));
        columnNames.add(new Tuple2("Education", 3));
        columnNames.add(new Tuple2("Education-num", 4));
        columnNames.add(new Tuple2("Marital-status", 5));
        columnNames.add(new Tuple2("Occupation", 6));
        columnNames.add(new Tuple2("Relationship", 7));
        columnNames.add(new Tuple2("Race", 8));
        columnNames.add(new Tuple2("Sex", 9));
        columnNames.add(new Tuple2("Capital-gain", 10));
        columnNames.add(new Tuple2("Capital-loss", 11));
        columnNames.add(new Tuple2("Hours-per-week", 12));
        columnNames.add(new Tuple2("Native-country", 13));
        columnNames.add(new Tuple2("Over-than-fifty", 14));
    }
    
    public ArrayList<Tuple2<String,Integer>> getColumnNames(){return columnNames;}
    
    public JavaRDD<Adult> parseFile(){
        
        JavaPairRDD<String, Long> temp = this.input.zipWithIndex();
        
        Broadcast<ArrayList<Tuple2<String,Integer>>> bColNames = Singleton.getSparkContext().broadcast(this.columnNames);
        
        JavaRDD<Adult> rdd_adults = temp.map(
            new Function<Tuple2<String, Long>, Adult>(){
                public Adult call(Tuple2<String, Long> tuple) throws Exception {
                    String[] fields = tuple._1.split(",");
                    //turn array to list
                    List<String> temp = ImmutableList.copyOf(fields);
                    ArrayList<String> fieldsList = new ArrayList<>(temp);
                    
                    Adult adult = new Adult(bColNames.value(), fieldsList, 
                            tuple._2.intValue());
                    return adult;
                }
            });

        return rdd_adults;
    }
}
