package com.poiitis.ducc;

import com.google.common.collect.ImmutableList;
import com.poiitis.utils.Singleton;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
    private int numOfColumns = -1;//read only a given number of columns
    
    public Parser(JavaRDD<String> input){    
        this.input = input;
        columnNames = new ArrayList<>();
        
        String[] columns = this.input.first().split(",");
        
            for(int i=0; i<columns.length; i++){
                columnNames.add(new Tuple2<>(columns[i], i));
            }
    }
    
    public Parser(JavaRDD<String> input, int numOfColumns){    
        this.input = input;
        columnNames = new ArrayList<>();
        this.numOfColumns = numOfColumns;
        
        String[] columns = this.input.first().split(",");
        
        //escape whitespaces
        for(int i=0; i<columns.length; i++){
                columns[i] = columns[i].trim();
        }
        
        if(numOfColumns == -1){
            for(int i=0; i<columns.length; i++){
                columnNames.add(new Tuple2<>(columns[i], i));
            }
        }
        else{
           for(int i=0; i<this.numOfColumns; i++){
                columnNames.add(new Tuple2<>(columns[i], i));
            } 
        }
        
    }
    
    public ArrayList<Tuple2<String,Integer>> getColumnNames(){return columnNames;}
    
    public JavaRDD<Adult> parseFile(){
        
        JavaPairRDD<String, Long> temp = this.input.zipWithIndex();
        temp = temp.filter((Tuple2<String, Long> t) -> (t._2 > 0));//escape header line of file
        
        Broadcast<ArrayList<Tuple2<String,Integer>>> bColNames = Singleton.getSparkContext().broadcast(this.columnNames);
        Broadcast<Integer> bNumOfColumns = Singleton.getSparkContext().broadcast(this.numOfColumns);
        
        JavaRDD<Adult> rdd_adults = temp.map((Tuple2<String, Long> tuple) -> {
            String[] fields = tuple._1.split(",");
            
            //escape whitespaces
            for(int i=0; i<fields.length; i++){
                fields[i] = fields[i].trim();
            }
            
            ArrayList<String> fieldsList;
            if(bNumOfColumns.value() == -1){
                fieldsList = new ArrayList<>(Arrays.asList(fields));
            }
            else{
                fieldsList = new ArrayList<>();
                for(int i=0; i<bNumOfColumns.value(); i++){
                    fieldsList.add(fields[i]);
                }
            }
            
            Adult adult = new Adult(bColNames.value(), fieldsList,
                    tuple._2.intValue());
            
            return adult;
        });

        return rdd_adults;
    }
}
