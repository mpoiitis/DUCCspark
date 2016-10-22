package com.poiitis.ducc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 *
 * @author Poiitis Marinos
 */
public class Adult implements Serializable{

    private static final long serialVersionUID = -4226228608985544865L;
    
    //linked hash map so as to preserve insertion order
    private LinkedHashMap<String,String> attributes;
    //the position in dataset
    private long lineNumber;
    
    public Adult(ArrayList<String> columnNames, ArrayList<String> fields, Long line){
        
        this.attributes = new LinkedHashMap<>();
        
        if(columnNames.size() == fields.size()){
            int i = 0;
            for(String name : columnNames){
                attributes.put(name, fields.get(i).trim());
                i++;
            }
        }
        
        this.lineNumber = line;
    }
    
    public LinkedHashMap<String,String> getAttributes(){return this.attributes;}
    public long getLineNumber(){return this.lineNumber;}
    
}
