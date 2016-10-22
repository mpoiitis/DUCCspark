package com.poiitis.ducc;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;

/**
 *
 * @author Poiitis Marinos
 */
public class Parser implements Serializable{

    private static final long serialVersionUID = -3808214153694185619L;
    
    private JavaRDD<String> input;
    private ArrayList<String> columnNames;
    
    public Parser(JavaRDD<String> input){    
        this.input = input;
        columnNames = new ArrayList<>();
        
        columnNames.add("Age");
        columnNames.add("Workclass");
        columnNames.add("Fnlwgt");
        columnNames.add("Education");
        columnNames.add("Education-num");
        columnNames.add("Marital-status");
        columnNames.add("Occupation");
        columnNames.add("Relationship");
        columnNames.add("Race");
        columnNames.add("Sex");
        columnNames.add("Capital-gain");
        columnNames.add("Capital-loss");
        columnNames.add("Hours-per-week");
        columnNames.add("Native-country");
        columnNames.add("Over-than-fifty");
    }
    
    public ArrayList<String> getColumnNames(){return columnNames;}
    
    public JavaRDD<Adult> parseFile(LongAccumulator lineNumber){
        
        JavaRDD<Adult> rdd_adults = input.map(
            new Function<String, Adult>(){
                public Adult call(String line) throws Exception {
                    String[] fields = line.split(",");
                    //turn array to list
                    List<String> temp = ImmutableList.copyOf(fields);
                    ArrayList<String> fieldsList = new ArrayList<>(temp);
                    
                    Adult adult = new Adult(columnNames, fieldsList, 
                            lineNumber.value());
                    lineNumber.add(1);
                    return adult;
                }
            });
        
        return rdd_adults;
    }
}
