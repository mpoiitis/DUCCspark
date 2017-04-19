package com.poiitis.ducc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import scala.Tuple2;

/**
 *
 * @author Poiitis Marinos
 */
public class Adult implements Serializable{

    private static final long serialVersionUID = -4226228608985544865L;
    
    //linked hash map so as to preserve insertion order
    private LinkedHashMap<Tuple2<String,Integer>,String> attributes;
    //the position in dataset
    private int lineNumber;
    
    public Adult(ArrayList<Tuple2<String,Integer>> columnNames, ArrayList<String> fields, Integer line){
        
        this.attributes = new LinkedHashMap<>();
        
        if(columnNames.size() == fields.size()){
            int i = 0;
            for(Tuple2<String,Integer> tuple : columnNames){
                attributes.put(tuple, fields.get(i).trim());
                i++;
            }
        }
        
        this.lineNumber = line;
    }
    
    public String toString(){
        return lineNumber + " " + attributes.toString();
    }
    
    public LinkedHashMap<Tuple2<String,Integer>,String> getAttributes(){return this.attributes;}
    public int getLineNumber(){return this.lineNumber;}
    
}
