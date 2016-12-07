package com.poiitis.graphUtils;

import com.poiitis.columns.ColumnCombinationBitset;
import com.poiitis.utils.Singleton;
import java.util.ArrayList;import java.util.Iterator;
;
import java.util.List;
import java.util.Set;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author Marinos Poiitis
 */
public class HoleFinder {
    protected JavaRDD<ColumnCombinationBitset> complementarySet;
    protected ColumnCombinationBitset allBitsSet = new ColumnCombinationBitset(new int[0]);

    public HoleFinder(int numberOfColumns) {
        this.allBitsSet.setAllBits(numberOfColumns);
    }

    public JavaRDD<ColumnCombinationBitset> getHoles() {
        return this.complementarySet;
    }

    /**
     * get holes according to complementary set that are not contained in given column combinations
     */
    public JavaRDD<ColumnCombinationBitset> getHolesWithoutGivenColumnCombinations(JavaRDD<ColumnCombinationBitset> givenColumnCombination) {
 
        JavaRDD<ColumnCombinationBitset> holes = this.complementarySet.subtract(givenColumnCombination);
        return holes;
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
    public void removeMinimalPositiveFromComplementarySet(ColumnCombinationBitset set) {
        List<ColumnCombinationBitset> sets = new ArrayList<>();
        sets.add(set);
        JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(sets);
        this.complementarySet = this.complementarySet.subtract(rdd);
    }

    public void update(ColumnCombinationBitset maximalNegative) {
        ColumnCombinationBitset singleComplementMaxNegative = this.allBitsSet.minus(maximalNegative);
        if (this.complementarySet.collect().size() == 0) {
            List<ColumnCombinationBitset> list = singleComplementMaxNegative.getContainedOneColumnCombinations();
            JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(list);
            this.complementarySet = this.complementarySet.union(rdd);
            
            return;
        }
        JavaRDD<ColumnCombinationBitset> complementarySetsArray = Singleton.getSparkContext().emptyRDD();
        this.addPossibleCombinationsToComplementArray(complementarySetsArray, singleComplementMaxNegative);
        this.removeSubsetsFromComplementarySetsArray(complementarySetsArray);
        this.complementarySet = complementarySetsArray.filter((ColumnCombinationBitset ccb) -> {
            return ccb != null ? true : false;
        });
        
        /*
        this.complementarySet.clear();
        for (ColumnCombinationBitset c : complementarySetsArray) {
            if (c == null) continue;
            this.complementarySet.add(c);
        }
        */
    }

    protected void removeSubsetsFromComplementarySetsArray(JavaRDD<ColumnCombinationBitset> complementarySetsList) {
        //get all possible combinations. just like nested for loops
        JavaPairRDD<ColumnCombinationBitset,ColumnCombinationBitset> cartesian = 
                complementarySetsList.cartesian(complementarySetsList);
        
        //rdd containing col comb bitset name as key and col comb bitset as value in order to reduce by key.
        //reduce: if either col comb bitset value is null return null, else the col comb bitset.
        JavaPairRDD<String, ColumnCombinationBitset> reformedCartesian = cartesian.mapToPair(new PairFunction<Tuple2<ColumnCombinationBitset, ColumnCombinationBitset>,
            String, ColumnCombinationBitset>(){
                public Tuple2<String, ColumnCombinationBitset> call(Tuple2<ColumnCombinationBitset, ColumnCombinationBitset> tuple){
                   
                    if(tuple._1 != tuple._2 && tuple._2 != null && tuple._2.containsSubset(tuple._1)){
                        return new Tuple2<>(tuple._2.toString(), null); 
                    }
                   
                    return new Tuple2<>(tuple._2.toString(), tuple._2);
                }     
            }).reduceByKey(new Function2<ColumnCombinationBitset, ColumnCombinationBitset, ColumnCombinationBitset>(){
                public ColumnCombinationBitset call(ColumnCombinationBitset ccb1, ColumnCombinationBitset ccb2){
                    if(ccb1 == null || ccb2 == null){
                        return null;
                    }
                    else{
                        return ccb1;
                    }
                }
            });
        
        complementarySetsList = reformedCartesian.map(new Function<Tuple2<String, ColumnCombinationBitset>, 
                ColumnCombinationBitset>(){
                    public ColumnCombinationBitset call(Tuple2<String, ColumnCombinationBitset> tuple){
                        return tuple._2;
                    }
                });
        /*
        for (int a = 0; a < complementarySetsList.size(); ++a) {
            if (complementarySetsList.get(a) == null) continue;
            for (int b = 0; b < complementarySetsList.size(); ++b) {
                if (a == b || complementarySetsList.get(b) == null || !complementarySetsList.get(b).containsSubset(complementarySetsList.get(a))) continue;
                complementarySetsList.set(b, null);
            }
        }
        */
    }

    protected void addPossibleCombinationsToComplementArray(JavaRDD<ColumnCombinationBitset> complementSet, ColumnCombinationBitset singleComplement) {
        List<ColumnCombinationBitset> oneColumnBitSetsOfSingleComplement = singleComplement.getContainedOneColumnCombinations();
        
        complementSet = this.complementarySet.flatMap(new FlatMapFunction<ColumnCombinationBitset, ColumnCombinationBitset>(){
            public Iterator<ColumnCombinationBitset> call(ColumnCombinationBitset ccb){
                ColumnCombinationBitset intersectedCombination = ccb.intersect(singleComplement);
                if(intersectedCombination.getSetBits().size() !=0){
                    ArrayList<ColumnCombinationBitset> list = new ArrayList<>();
                    list.add(ccb);
                    return list.iterator();
                }
                ArrayList<ColumnCombinationBitset> list = new ArrayList<>();
                for (ColumnCombinationBitset oneColumnBitSet : oneColumnBitSetsOfSingleComplement) {
                    list.add(ccb.union(oneColumnBitSet));
                }
                return list.iterator();
            }   
        });
        
        /*
        for (ColumnCombinationBitset set : this.complementarySet) {
            ColumnCombinationBitset intersectedCombination = set.intersect(singleComplement);
            if (intersectedCombination.getSetBits().size() != 0) {
                complementSet.add(set);
                continue;
            }
            for (ColumnCombinationBitset oneColumnBitSet : oneColumnBitSetsOfSingleComplement) {
                complementSet.add(set.union(oneColumnBitSet));
            }
        }*/
    }
}
