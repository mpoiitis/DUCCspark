package com.poiitis.ducc;

import java.io.Serializable;
import java.util.ArrayList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

/**
 *
 * @author Poiitis Marinos
 */
public class Parser implements Serializable{

    private static final long serialVersionUID = -3808214153694185619L;
    
    private JavaRDD<String> input; 
    
    public Parser(JavaRDD<String> input){    
        this.input = input;
    }
    
    public ArrayList<String> getColumnNames(){
        
        ArrayList<String> columnNames = new ArrayList<>();
        
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
        
        return columnNames;
    }
    
    public JavaRDD<Adult> parseFile(){
        
        JavaRDD<Adult> rdd_adults = input.map(
            new Function<String, Adult>(){
                public Adult call(String line) throws Exception {
                    String[] fields = line.split(",");
                    Adult adult = new Adult(fields[0].trim(),fields[1].trim(),
                        fields[2].trim(),fields[3].trim(),fields[4].trim(),
                        fields[5].trim(),fields[6].trim(),fields[7].trim(),
                        fields[8].trim(),fields[9].trim(),fields[10].trim(),
                        fields[11].trim(),fields[12].trim(),fields[13].trim(),
                        fields[14].trim());
                    return adult;
                }
            });
        
        return rdd_adults;
    }
}
