package com.poiitis.ducc;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;
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
    
    public JavaRDD<Adult> parseFile(LongAccumulator lineNumber){
        
        JavaRDD<Adult> rdd_adults = input.map(
            new Function<String, Adult>(){
                public Adult call(String line) throws Exception {
                    String[] fields = line.split(",");
                    //turn array to list
                    List<String> temp = ImmutableList.copyOf(fields);
                    ArrayList<String> fieldsList = new ArrayList<>(temp);
                    
                    Adult adult = new Adult(columnNames, fieldsList, 
                            lineNumber.value().intValue());
                    lineNumber.add(1);
                    return adult;
                }
            });
        
        return rdd_adults;
    }
}
