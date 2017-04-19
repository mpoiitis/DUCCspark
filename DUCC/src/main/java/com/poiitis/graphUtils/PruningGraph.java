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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *
 * @author Marinos Poiitis
 */
public class PruningGraph implements Serializable{

    private static final long serialVersionUID = 5978189643146957123L;
    protected final int OVERFLOW_THRESHOLD;
    protected final boolean containsPositiveFeature;
    
    //dummy rdd needed only for columnCombinationMap initialization
    private JavaRDD<Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>>> dummy = Singleton.getSparkContext().emptyRDD();
    
    protected JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> columnCombinationMap = 
            JavaPairRDD.fromJavaRDD(dummy);
    protected JavaRDD<ColumnCombinationBitset> overflow = Singleton.getSparkContext().emptyRDD();
    protected final int numberOfColumns;
    protected ColumnCombinationBitset allBitsSet;

    public PruningGraph(int numberOfColumns, int overflowTreshold, boolean positiveFeature) {
        this.columnCombinationMap.cache();
        this.OVERFLOW_THRESHOLD = overflowTreshold;
        this.numberOfColumns = numberOfColumns;
        this.containsPositiveFeature = positiveFeature;
        int[] setBits = new int[this.numberOfColumns];
        int i = 0;
        while (i < this.numberOfColumns) {
            setBits[i] = i++;
        }
        this.allBitsSet = new ColumnCombinationBitset(setBits);
    }

    public void add(ColumnCombinationBitset columnCombination){

        System.out.println(columnCombination.toString() + " add");
        if (this.containsPositiveFeature) {
            List<ColumnCombinationBitset> list = columnCombination.getAllSupersets(this.numberOfColumns);
            for (int i = 0; i < list.size(); ++i) {
                if(i%10 == 0){
                    this.columnCombinationMap = this.columnCombinationMap.coalesce(3);
                    this.columnCombinationMap = this.columnCombinationMap.cache();
                    this.columnCombinationMap.checkpoint();
                }
                this.addToKey(list.get(i), columnCombination);  
            }
        } else {
            List<ColumnCombinationBitset> list = columnCombination.getAllSubsets();
            for (int i = 0; i < list.size(); ++i) {
                if(i%10 == 0){
                    this.columnCombinationMap = this.columnCombinationMap.coalesce(3);
                    this.columnCombinationMap = this.columnCombinationMap.cache();
                    this.columnCombinationMap.checkpoint();
                }
                this.addToKey(list.get(i), columnCombination);
            }
        }
        System.out.println("End of add");
    }
    
    public void addToKey(ColumnCombinationBitset key, ColumnCombinationBitset columnCombination){
        //take only the ones with the given key
        Broadcast<ColumnCombinationBitset> bKey = Singleton.getSparkContext().broadcast(key);
        
        JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> rdd = this.columnCombinationMap.filter((Tuple2<ColumnCombinationBitset,
                List<ColumnCombinationBitset>> tuple) ->{ 
            return tuple._1.equals(bKey.value());
        });
        
        rdd.cache();
        
        //needed for else if below. there is only one element in rdd so get(0) returns it.
        List<ColumnCombinationBitset> columnCombinationList = new ArrayList<>();
        if(!rdd.isEmpty()){
            columnCombinationList = rdd.map((Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> t) ->
            {
                return t._2;
            }).collect().get(0);
        }
        
        if(rdd.isEmpty()){
            System.out.println(key.toString() + " empty add");
            List<ColumnCombinationBitset> list = new ArrayList<>();
            list.add(columnCombination);
            
            Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> tuple = new Tuple2<>(key, list);
           
            List<Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>>> tupleList = new ArrayList<>();
            tupleList.add(tuple);
            
            JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> unionRdd = Singleton.getSparkContext().parallelizePairs(tupleList);
            
            //simulate put method of hashmap. Doesnt need concat on key because there was not such key
            this.columnCombinationMap = this.columnCombinationMap.union(unionRdd);
        }
        else if(!columnCombinationList.contains((Object) columnCombination)){
            Iterator<ColumnCombinationBitset> iterator = columnCombinationList.iterator();
            while(iterator.hasNext()){
                ColumnCombinationBitset currentBitset = iterator.next();
                if(this.containsPositiveFeature){
                    if(columnCombination.containsSubset(currentBitset)){
                        return;
                    }
                    if(!columnCombination.isSubsetOf(currentBitset)) continue;
                    iterator.remove();
                    continue;
                }
                if(columnCombination.isSubsetOf(currentBitset)){
                    return;
                }
                if (!columnCombination.containsSubset(currentBitset)) continue;
                iterator.remove();
            }
            columnCombinationList.add(columnCombination);
            
            if(columnCombinationList.size() >= this.OVERFLOW_THRESHOLD){
                System.out.println(key.toString() + " not empty overflow add");
                Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> tuple = new Tuple2<>(key, this.overflow.collect());
                List<Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>>> tupleList = new ArrayList<>();
                tupleList.add(tuple);
                
                JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> unionRdd = Singleton.getSparkContext().parallelizePairs(tupleList);
                
                this.columnCombinationMap = this.columnCombinationMap.union(unionRdd).reduceByKey((List<ColumnCombinationBitset> l1, List<ColumnCombinationBitset> l2) -> {
                    for(ColumnCombinationBitset c : l2){
                        l1.add(c);
                    }
                    return l1;
                });

            }
            else{
                System.out.println(key.toString() + " not empty add");
                List<ColumnCombinationBitset> list = new ArrayList<>();
                list.add(columnCombination);
                
                Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> tuple = new Tuple2<>(key, list);
                
                List<Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>>> tupleList = new ArrayList<>();
                tupleList.add(tuple);
                
                JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> unionRdd = Singleton.getSparkContext().parallelizePairs(tupleList);
                
                this.columnCombinationMap = this.columnCombinationMap.union(unionRdd).reduceByKey((List<ColumnCombinationBitset> l1, List<ColumnCombinationBitset> l2) -> {
                    for(ColumnCombinationBitset c : l2){
                        l1.add(c);
                    }
                    return l1;
                });
            }
        }
        else{
            System.out.println(key.toString() + " didn't add anything");
        }
        
        //Destroy broadcast variables
        bKey.destroy();
    }
    
   /* public void add(ColumnCombinationBitset columnCombination) {
        System.out.println(columnCombination.toString() + " add");
        int initialKeyLength = 1;
        for (int i = 0; i < columnCombination.getNSubsetColumnCombinations(initialKeyLength).size(); ++i) {
            this.addToKey((ColumnCombinationBitset)columnCombination.getNSubsetColumnCombinations(initialKeyLength).get(i), columnCombination, initialKeyLength);
        }
        System.out.println("end of add");
    }

    protected void addToKey(ColumnCombinationBitset key, ColumnCombinationBitset columnCombination, int keyLength) {
        //take only the ones with the given key
        Broadcast<ColumnCombinationBitset> bKey = Singleton.getSparkContext().broadcast(key);
        
        JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> rdd = this.columnCombinationMap.filter((Tuple2<ColumnCombinationBitset,
                List<ColumnCombinationBitset>> tuple) ->{ 
            return tuple._1.equals(bKey.value());
        });
        
        rdd.cache();
        
        //needed for else if below. there is only one element in rdd so get(0) returns it.
        List<ColumnCombinationBitset> columnCombinationList = new ArrayList<>();
        if(!rdd.isEmpty()){
            columnCombinationList = rdd.map((Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> t) ->
            {
                return t._2;
            }).collect().get(0);
        }
        
        if(rdd.isEmpty()){
            System.out.println(key.toString() + " empty add");
            List<ColumnCombinationBitset> list = new ArrayList<>();
            list.add(columnCombination);
            
            Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> tuple = new Tuple2<>(key, list);
           
            List<Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>>> tupleList = new ArrayList<>();
            tupleList.add(tuple);
            
            JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> unionRdd = Singleton.getSparkContext().parallelizePairs(tupleList);
            
            //simulate put method of hashmap. Doesnt need concat on key because there was not such key
            this.columnCombinationMap = this.columnCombinationMap.union(unionRdd);
            
            this.addToSubKey(columnCombination, keyLength + 1);
        }
        else if(this.overflow.collect() == columnCombinationList){
            System.out.println(key.toString() + " overflow add");
            this.addToSubKey(columnCombination, keyLength + 1);
        }
        else if(!columnCombinationList.contains((Object) columnCombination)){
            Iterator<ColumnCombinationBitset> iterator = columnCombinationList.iterator();
            while(iterator.hasNext()){
                ColumnCombinationBitset currentBitset = iterator.next();
                if(this.containsPositiveFeature){
                    if(columnCombination.containsSubset(currentBitset)){
                        return;
                    }
                    if(!columnCombination.isSubsetOf(currentBitset)) continue;
                    iterator.remove();
                    continue;
                }
                if(columnCombination.isSubsetOf(currentBitset)){
                    return;
                }
                if (!columnCombination.containsSubset(currentBitset)) continue;
                iterator.remove();
            }
            columnCombinationList.add(columnCombination);
            
            if(columnCombinationList.size() >= this.OVERFLOW_THRESHOLD){
                System.out.println(key.toString() + " not empty overflow add");
                Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> tuple = new Tuple2<>(key, this.overflow.collect());
                List<Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>>> tupleList = new ArrayList<>();
                tupleList.add(tuple);
                
                JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> unionRdd = Singleton.getSparkContext().parallelizePairs(tupleList);
                
                this.columnCombinationMap = this.columnCombinationMap.union(unionRdd).reduceByKey((List<ColumnCombinationBitset> l1, List<ColumnCombinationBitset> l2) -> {
                    for(ColumnCombinationBitset c : l2){
                        l1.add(c);
                    }
                    return l1;
                });
                
                for (ColumnCombinationBitset subCombination : columnCombinationList) {
                    this.addToSubKey(subCombination, keyLength + 1);
                }
            }
            else{
                System.out.println(key.toString() + " not empty add");
                List<ColumnCombinationBitset> list = new ArrayList<>();
                list.add(columnCombination);
                
                Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> tuple = new Tuple2<>(key, list);
                
                List<Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>>> tupleList = new ArrayList<>();
                tupleList.add(tuple);
                
                JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> unionRdd = Singleton.getSparkContext().parallelizePairs(tupleList);
                
                this.columnCombinationMap = this.columnCombinationMap.union(unionRdd).reduceByKey((List<ColumnCombinationBitset> l1, List<ColumnCombinationBitset> l2) -> {
                    for(ColumnCombinationBitset c : l2){
                        l1.add(c);
                    }
                    return l1;
                });
                for (ColumnCombinationBitset subCombination : columnCombinationList) {
                    this.addToSubKey(subCombination, keyLength + 1);
                }
            }
        }
        else{
            System.out.println(key.toString() + " didn't add anything");
        }
        
        //Destroy broadcast variables
        bKey.destroy();
    }

    protected void addToSubKey( ColumnCombinationBitset columnCombination, int keyLength) {
        for (int i = 0; i < columnCombination.getNSubsetColumnCombinations(keyLength).size(); ++i) {
            this.addToKey((ColumnCombinationBitset)columnCombination.getNSubsetColumnCombinations(keyLength).get(i), columnCombination, keyLength);
        }
    }
    */
    public boolean find(ColumnCombinationBitset columnCombination) {
        printGraph("");
        System.out.println(columnCombination.toString());
        return this.findRecursively(columnCombination, new ColumnCombinationBitset(new int[0]), 1);
    }

    protected boolean findRecursively(ColumnCombinationBitset columnCombination, ColumnCombinationBitset subset, int n) {
        List<ColumnCombinationBitset> keySetList = columnCombination.size() <= n ?
                this.allBitsSet.getNSubsetColumnCombinationsSupersetOf(columnCombination, n) : 
                columnCombination.getNSubsetColumnCombinationsSupersetOf(subset, n);
        
        for (ColumnCombinationBitset keySet : keySetList) {
            
            List<ColumnCombinationBitset> currentColumnCombinations;
            if(this.columnCombinationMap.isEmpty()){
                currentColumnCombinations = null;
            }
            else{
                Broadcast<ColumnCombinationBitset> bKeySet = Singleton.getSparkContext().broadcast(keySet);
                JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> check = this.columnCombinationMap.filter((Tuple2<ColumnCombinationBitset,
                    List<ColumnCombinationBitset>> tuple) -> tuple._1.equals((Object) bKeySet.value()));
                if(check.isEmpty()){
                    currentColumnCombinations = null;
                }
                else{
                    currentColumnCombinations = check.values().first();
                }
                bKeySet.destroy();
            }

            //List<ColumnCombinationBitset> currentColumnCombinations = this.columnCombinationMap.get((Object)keySet);
            if (!this.equalLists(currentColumnCombinations, this.overflow.collect()) && currentColumnCombinations != null) {
                for (ColumnCombinationBitset currentColumnBitset : currentColumnCombinations) {
                    
                    if (!(this.containsPositiveFeature ? columnCombination.containsSubset(currentColumnBitset) :
                            columnCombination.isSubsetOf(currentColumnBitset))) continue;
                    System.out.println("found");
                    return true;
                }
                continue;
            }
            if (!this.equalLists(currentColumnCombinations, this.overflow.collect()) || !this.findRecursively(columnCombination, keySet, n + 1)) continue;
            System.out.println("found");
            return true;
        }
        System.out.println("not found");
        return false;
    }
    
    protected  boolean equalLists(List<ColumnCombinationBitset> one, List<ColumnCombinationBitset> two){     
        if (one == null && two == null){
            return true;
        }

        if((one == null && two != null) || (one != null && two == null) || one.size() != two.size()){
            return false;
        }

        one = new ArrayList<ColumnCombinationBitset>(one); 
        two = new ArrayList<ColumnCombinationBitset>(two);   

        Collections.sort(one);
        Collections.sort(two);      
        return one.equals(two);
    }
    
    protected void printGraph(String str){
        String type = this.containsPositiveFeature ? "POSITIVE" : "NEGATIVE";
        System.out.println(type + " COLUMN COMBINATION MAP " + str + " =================================");
        for(Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> t : this.columnCombinationMap.collect()){
            System.out.println(t._1.toString());
        }
        System.out.println(type + " COLUMN COMBINATION MAP END=================================");
    }
}
