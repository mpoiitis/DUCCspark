package com.poiitis.graphUtils;

import com.poiitis.columns.ColumnCombinationBitset;
import com.poiitis.utils.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author Marinos Poiitis
 */
public class PruningGraph {
    protected final int OVERFLOW_THRESHOLD;
    protected final boolean containsPositiveFeature;
    protected JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> columnCombinationMap;
    protected JavaRDD<ColumnCombinationBitset> overflow;
    protected final int numberOfColumns;
    protected ColumnCombinationBitset allBitsSet;

    public PruningGraph(int numberOfColumns, int overflowTreshold, boolean positiveFeature) {
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

    public void add(ColumnCombinationBitset columnCombination) {
        int initialKeyLength = 1;
        for (int i = 0; i < columnCombination.getNSubsetColumnCombinations(initialKeyLength).size(); ++i) {
            this.addToKey((ColumnCombinationBitset)columnCombination.getNSubsetColumnCombinations(initialKeyLength).get(i), columnCombination, initialKeyLength);
        }
    }

    protected void addToKey(ColumnCombinationBitset key, ColumnCombinationBitset columnCombination, int keyLength) {
        //take only the ones with the given key
        JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> rdd = this.columnCombinationMap.filter((Tuple2<ColumnCombinationBitset,
                List<ColumnCombinationBitset>> tuple) -> tuple._1.equals(key));

        JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> reformedRdd = rdd.mapToPair(new PairFunction<Tuple2<ColumnCombinationBitset,
                List<ColumnCombinationBitset>>, ColumnCombinationBitset, List<ColumnCombinationBitset>>(){
            public Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> call(Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> tuple){
                List<ColumnCombinationBitset> columnCombinationList = tuple._2;
                if(columnCombinationList == null){
                    columnCombinationList = new LinkedList<>();
                    columnCombinationList.add(columnCombination);
                    return new Tuple2<>(tuple._1, columnCombinationList);
                }
                else if(overflow.collect() == columnCombinationList){
                    addToSubKey(key, columnCombination, keyLength + 1);
                }
                else if(!columnCombinationList.contains((Object)columnCombination)){
                    Iterator<ColumnCombinationBitset> iterator = columnCombinationList.iterator();
                    while (iterator.hasNext()) {
                        ColumnCombinationBitset currentBitSet = iterator.next();
                        if (containsPositiveFeature) {
                            if (columnCombination.containsSubset(currentBitSet)) {
                                return tuple;//simple return
                            }
                            if (!columnCombination.isSubsetOf(currentBitSet)) continue;
                            iterator.remove();
                            continue;
                        }
                        if (columnCombination.isSubsetOf(currentBitSet)) {
                            return tuple;
                        }
                        if (!columnCombination.containsSubset(currentBitSet)) continue;
                        iterator.remove();
                    }
                    columnCombinationList.add(columnCombination);
                    if (columnCombinationList.size() >= OVERFLOW_THRESHOLD) {
                        List<ColumnCombinationBitset> unionList = overflow.collect();
                        //convert ccb to list(ccb). convert to pair with key,list(ccb)
                        JavaPairRDD<ColumnCombinationBitset, List<ColumnCombinationBitset>> unionRdd =
                            Singleton.getSparkContext().parallelize(unionList).map(new Function<ColumnCombinationBitset, List<ColumnCombinationBitset>>(){
                                public List<ColumnCombinationBitset> call(ColumnCombinationBitset ccb){
                                    List<ColumnCombinationBitset> list = new ArrayList<>();
                                    list.add(ccb);
                                    return list;
                                }
                            }).mapToPair(new PairFunction<List<ColumnCombinationBitset>,
                                ColumnCombinationBitset, List<ColumnCombinationBitset>>(){
                                    public Tuple2<ColumnCombinationBitset, List<ColumnCombinationBitset>> call(List<ColumnCombinationBitset> list){
                                        return new Tuple2<>(key, list);
                                    }
                                });

                        columnCombinationMap.union(unionRdd);
                        //combine lists of same key to one list
                        columnCombinationMap = columnCombinationMap.reduceByKey(new Function2<List<ColumnCombinationBitset>,
                            List<ColumnCombinationBitset>, List<ColumnCombinationBitset>>(){
                                public List<ColumnCombinationBitset> call(List<ColumnCombinationBitset> l1, List<ColumnCombinationBitset> l2){
                                    l1.addAll(l2);
                                    return l1;
                                }
                        });//final rdd: key,list(ccb)
                        for (ColumnCombinationBitset subCombination : columnCombinationList) {
                            addToSubKey(key, subCombination, keyLength + 1);
                        }
                    }
                }
                return tuple;//return input
            }
        });
    }

}
