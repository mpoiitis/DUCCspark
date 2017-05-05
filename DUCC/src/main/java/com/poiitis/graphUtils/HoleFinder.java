package com.poiitis.graphUtils;

import com.poiitis.columns.ColumnCombinationBitset;
import com.poiitis.utils.Singleton;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *
 * @author Marinos Poiitis
 */
public class HoleFinder implements Serializable{

    private static final long serialVersionUID = 6776853479213293743L;
    protected JavaRDD<ColumnCombinationBitset> complementarySet = Singleton.getSparkContext().emptyRDD();
    protected ColumnCombinationBitset allBitsSet = new ColumnCombinationBitset(new int[0]);

    public HoleFinder(int numberOfColumns) {
        this.allBitsSet.setAllBits(numberOfColumns);
    }

    public JavaRDD<ColumnCombinationBitset> getHoles() {
        this.complementarySet = this.complementarySet.cache();
        return this.complementarySet;
    }

    /**
     * get holes according to complementary set that are not contained in given column combinations
     */
    public JavaRDD<ColumnCombinationBitset> getHolesWithoutGivenColumnCombinations(JavaRDD<ColumnCombinationBitset> givenColumnCombination) {
        
        if(givenColumnCombination.isEmpty()){
            return this.getHoles();
        }
        
        if(this.complementarySet.isEmpty()){
            return Singleton.getSparkContext().emptyRDD();//return an empty rdd
        }
        else{ 
            JavaRDD<ColumnCombinationBitset> holes = this.complementarySet.subtract(givenColumnCombination);
            holes = holes.cache();
            return holes;
        }
    }
    
    /**
     * remove a given list of column combinations from complementary set 
     */
    public void removeMinimalPositivesFromComplementarySet(List<ColumnCombinationBitset> sets) {
        
        JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(sets);
        this.complementarySet = this.complementarySet.subtract(rdd);
    }

    /**
     * remove a column combination from complementary set 
     */
    public void removeMinimalPositiveFromComplementarySet(ColumnCombinationBitset ccb) {
        
        List<ColumnCombinationBitset> sets = new ArrayList<>();
        sets.add(ccb);
        JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(sets);
        this.complementarySet = this.complementarySet.subtract(rdd);
    }

    public void update(ColumnCombinationBitset maximalNegative) {

        System.out.println("Marinos " + maximalNegative);
        ColumnCombinationBitset singleComplementMaxNegative = this.allBitsSet.minus(maximalNegative);
        if (this.complementarySet.isEmpty()) {
            List<ColumnCombinationBitset> list = singleComplementMaxNegative.getContainedOneColumnCombinations();
            JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(list);
            
            this.complementarySet = this.complementarySet.union(rdd);
           
            System.out.println("Marinos complementary set from empty ");
            for(ColumnCombinationBitset c : this.complementarySet.collect()){
                System.out.println("Marinos complement " + c);
            }

            return;
        }
        JavaRDD<ColumnCombinationBitset> complementarySetsArray = this.addPossibleCombinationsToComplementArray(singleComplementMaxNegative);
        
        complementarySetsArray = this.removeSubsetsFromComplementarySetsArray(complementarySetsArray);
        
        this.complementarySet = complementarySetsArray.filter((ColumnCombinationBitset ccb) -> {
            return ccb != null ? true : false;
        });
        
        System.out.println("Marinos complementary set");
        for(ColumnCombinationBitset c : this.complementarySet.collect()){
            System.out.println("Marinos complement " + c);
        }
    }

    protected JavaRDD<ColumnCombinationBitset> removeSubsetsFromComplementarySetsArray(JavaRDD<ColumnCombinationBitset> complementarySetsList) {
        //get all possible combinations. just like nested for loops
        JavaPairRDD<ColumnCombinationBitset,ColumnCombinationBitset> cartesian = 
                complementarySetsList.cartesian(complementarySetsList);
        
        //rdd containing col comb bitset name as key and col comb bitset as value in order to reduce by key.
        //reduce: if either col comb bitset value is null return null, else the col comb bitset.
        JavaPairRDD<String, ColumnCombinationBitset> reformedCartesian = cartesian.mapToPair((Tuple2<ColumnCombinationBitset, ColumnCombinationBitset> tuple) -> {
            if(!tuple._1.equals(tuple._2) && tuple._2 != null && tuple._2.containsSubset(tuple._1)){
                return new Tuple2<>(tuple._2.toString(), null);
            }
            return new Tuple2<>(tuple._2.toString(), tuple._2);
        }).reduceByKey((ColumnCombinationBitset ccb1, ColumnCombinationBitset ccb2) -> {
            if(ccb1 == null || ccb2 == null){
                return null;
            }
            else{
                return ccb1;
            }
        });
        
        complementarySetsList = reformedCartesian.map((Tuple2<String, ColumnCombinationBitset> tuple) -> tuple._2);
        complementarySetsList = complementarySetsList.cache();
        return complementarySetsList;
    }

    protected JavaRDD<ColumnCombinationBitset> addPossibleCombinationsToComplementArray(ColumnCombinationBitset singleComplement) {
        List<ColumnCombinationBitset> oneColumnBitSetsOfSingleComplement = singleComplement.getContainedOneColumnCombinations();
        
        List<Broadcast<ColumnCombinationBitset>> bList = new ArrayList<>();
        for(ColumnCombinationBitset c : oneColumnBitSetsOfSingleComplement){
            Broadcast<ColumnCombinationBitset> bC = Singleton.getSparkContext().broadcast(c);
            bList.add(bC);
        }
        
        Broadcast<ColumnCombinationBitset> bSingleComplement = Singleton.getSparkContext().broadcast(singleComplement);
        
        JavaRDD<ColumnCombinationBitset> complementSet = this.complementarySet.flatMap((ColumnCombinationBitset ccb) -> {
            ColumnCombinationBitset intersectedCombination = ccb.intersect(bSingleComplement.value());
            if(intersectedCombination.getSetBits().size() !=0){
                ArrayList<ColumnCombinationBitset> list = new ArrayList<>();
                list.add(ccb);
                return list.iterator();
            }   
            ArrayList<ColumnCombinationBitset> list = new ArrayList<>();
            for (Broadcast<ColumnCombinationBitset> oneColumnBitSet : bList) {
                list.add(ccb.union(oneColumnBitSet.value()));
            }
            return list.iterator();
        });

        return complementSet;
    }
}
