/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.poiitis.graphUtils;

import com.poiitis.columns.ColumnCombinationBitset;
import com.poiitis.utils.Singleton;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;

/**
 *
 * @author Marinos Poiitis
 */
public class SimplePruningGraph implements Serializable{

    private static final long serialVersionUID = 5978189643146957123L;
    protected final boolean containsPositiveFeature;
    protected JavaRDD<ColumnCombinationBitset> columnCombinationMap = Singleton.getSparkContext().emptyRDD();
    protected final int numberOfColumns;
    protected ColumnCombinationBitset allBitsSet;

    public SimplePruningGraph(int numberOfColumns, boolean positiveFeature) {
        this.numberOfColumns = numberOfColumns;
        this.containsPositiveFeature = positiveFeature;
        int[] setBits = new int[this.numberOfColumns];
        int i = 0;
        while (i < this.numberOfColumns) {
            setBits[i] = i++;
        }
        this.allBitsSet = new ColumnCombinationBitset(setBits);
    }

    
    /**
     * if this is a uniques graph, add column combination and all of its supersets,
     * else add column combination and all of its subsets
     * @param columnCombination 
     */
    public void add(ColumnCombinationBitset columnCombination){
        
        List<ColumnCombinationBitset> list = new ArrayList<>();
        
        if (this.containsPositiveFeature) {
            list = columnCombination.getAllSupersets(this.numberOfColumns);
        }
        else{
            list = columnCombination.getAllSubsetsNoZero();
        }
        
        this.columnCombinationMap = this.columnCombinationMap.coalesce(3);
        
        JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(list);
        this.columnCombinationMap = this.columnCombinationMap.union(rdd).distinct();

        this.columnCombinationMap = this.columnCombinationMap.cache();
        
    }

    protected boolean find(ColumnCombinationBitset columnCombination) {
        
        if(this.columnCombinationMap.isEmpty()){
            return false;
        }
        else{
            Broadcast<ColumnCombinationBitset> bCcb = Singleton.getSparkContext().broadcast(columnCombination);
            JavaRDD<ColumnCombinationBitset> check = this.columnCombinationMap.filter((ColumnCombinationBitset ccb) -> ccb.equals((Object) bCcb.value()));
            if(check.isEmpty()){
                bCcb.destroy();
                return false;
            }
            else{
                bCcb.destroy();
                return true;
            }
            
        }
    }
    
    protected void printGraph(String str){
        String type = this.containsPositiveFeature ? "POSITIVE" : "NEGATIVE";
        System.out.println(type + " COLUMN COMBINATION MAP " + str + " =================================");
        for(ColumnCombinationBitset ccb : this.columnCombinationMap.collect()){
            System.out.println(ccb.toString());
        }
        System.out.println(type + " COLUMN COMBINATION MAP END=================================");
    }
}
