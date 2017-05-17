/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.poiitis.ducc;

import com.poiitis.utils.Singleton;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *Split by any tab. Read column names from first line
 * @author Poiitis Marinos
 */
public class AutoParser implements Serializable{
    
    private static final long serialVersionUID = -4474826565180952044L;
    
    private JavaRDD<String> input;
    private ArrayList<Tuple2<String,Integer>> columnNames;
    private int numOfColumns = -1;//read only a given number of columns
    
    public AutoParser(JavaRDD<String> input){    
        this.input = input;
        columnNames = new ArrayList<>();
        
        String[] columns = this.input.first().split("\\s+");
        
            for(int i=0; i<columns.length; i++){
                columnNames.add(new Tuple2<>(columns[i], i));
            }
    }
    
    public AutoParser(JavaRDD<String> input, int numOfColumns){    
        this.input = input;
        columnNames = new ArrayList<>();
        this.numOfColumns = numOfColumns;
        
        String[] columns = this.input.first().split("\\s+");
        
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
            String[] fields = tuple._1.split("\\t");
            
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
