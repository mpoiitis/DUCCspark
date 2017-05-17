package com.poiitis.graphUtils;

import com.poiitis.columns.ColumnCombinationBitset;
import com.poiitis.exceptions.ColumnNameMismatchException;
import com.poiitis.exceptions.CouldNotReceiveResultException;
import com.poiitis.pli.PositionListIndex;
import com.poiitis.utils.Singleton;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 *
 * @author Marinos Poiitis
 */
public class SimpleUccGraphTraverser
extends SimpleGraphTraverser implements Serializable {

    private static final long serialVersionUID = -1252806705177680171L;
    protected List<Tuple2<String,Integer>> columnNames;
    protected long desiredKeyError = 0;

    public SimpleUccGraphTraverser() {
    }

    public SimpleUccGraphTraverser(Random random) {
        this();
        this.random = random;
    }

    public void saveResults(String outputPath){
        if(!this.minimalPositives.isEmpty()){
            this.minimalPositives.coalesce(1, true).saveAsTextFile(outputPath);
        }
        else{
            List<String> outputList = new ArrayList<>();
            outputList.add("No unique column combinations found!");
            JavaRDD<String> output = Singleton.getSparkContext().parallelize(outputList);
            output.coalesce(1).saveAsTextFile(outputPath);
        }
    }
    
    public void setDesiredKeyError(long desiredKeyError) {
        this.desiredKeyError = desiredKeyError;
    }

    public void init(JavaRDD<PositionListIndex> basePLIs, List<Tuple2<String,Integer>> columnNames) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        this.columnNames = columnNames;
        this.filterNonUniqueColumnCombinationBitsets(basePLIs);
        this.numberOfColumns = (int) this.calculatedPlis.count();
        this.negativeGraph = new SimplePruningGraph(this.numberOfColumns, false);
        this.positiveGraph = new SimplePruningGraph(this.numberOfColumns, true);
        this.randomWalkTrace = new LinkedList<>();               
        this.seedCandidates = this.buildInitialSeeds(); 
        this.holeFinder = new SimpleHoleFinder(this.numberOfColumns);
    }

    /**
     * create a bitmask that contains 1's on the position of the columns that aren't unique.
     */
    protected void filterNonUniqueColumnCombinationBitsets(JavaRDD<PositionListIndex> basePLIs) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        
        this.calculatedPlis = basePLIs.mapToPair((PositionListIndex pli) -> {
            //there is always a single column in pli as it is basepli
            Tuple2<String,Integer> tuple = pli.getName().get(0);
            ColumnCombinationBitset currentColumnCombination = new ColumnCombinationBitset(new int[]{tuple._2});
            return new Tuple2<>(currentColumnCombination, pli);
        });
        
        this.calculatedPlis = this.calculatedPlis.cache();

        //keep only unique plis and add them to minimal graph
        this.minimalPositives = this.calculatedPlis.filter((Tuple2<ColumnCombinationBitset,
                PositionListIndex> tuple) -> isUnique(tuple._2)).map(new Function<Tuple2<ColumnCombinationBitset, PositionListIndex>, ColumnCombinationBitset>() {
            @Override
            public ColumnCombinationBitset call(Tuple2<ColumnCombinationBitset,
                    PositionListIndex> tuple) throws Exception {
                return tuple._1;
            }
        });

        //take all non-unique plis and create the bitmask for them
        JavaPairRDD<ColumnCombinationBitset, PositionListIndex> nonUniques = this.calculatedPlis.filter((Tuple2<ColumnCombinationBitset,
            PositionListIndex> tuple) -> (!isUnique(tuple._2)));
        
        this.bitmaskForNonUniqueColumns = nonUniques.aggregate(new ColumnCombinationBitset(), (ColumnCombinationBitset c, Tuple2<ColumnCombinationBitset,PositionListIndex> t) -> {
            //again we know that pli consists of a single column, thats why get(0)
            c.addColumn(t._2.getName().get(0)._2);
            return c;
        }, (ColumnCombinationBitset c1, ColumnCombinationBitset c2) -> {
            for(int i : c2.getSetBits()){
                c1.addColumn(i);
            }
            return c1;
        });
        
        /*for(Tuple2<ColumnCombinationBitset, PositionListIndex> tuple : nonUniques.collect()){
            //again we know that pli consists of a single column, thats why get(0)
            this.bitmaskForNonUniqueColumns.addColumn(tuple._2.getName().get(0)._2);//take the column index
        }*/

    }

    /**
     * calculate all possible seeds based on the already calculated bitmask 
     */
    protected JavaRDD<ColumnCombinationBitset> buildInitialSeeds() {
        
        List<ColumnCombinationBitset> list = this.bitmaskForNonUniqueColumns.getNSubsetColumnCombinations(2);

        JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(list);
        return rdd;
    }

    protected boolean isPositiveColumnCombination(ColumnCombinationBitset currentColumnCombination) {
        return this.isUnique(this.getPLIFor(currentColumnCombination));
    }

    protected boolean isAdditionalConditionTrueForFindUnprunedSetAndUpdateGivenList(ColumnCombinationBitset singleSet) {
        return false;
    }

    protected boolean isUnique(PositionListIndex pli) {
        if(pli == null){
            return false;
        }
        if (this.desiredKeyError == 0) {
            return pli.isUnique();
        }
        if (pli.getRawKeyError() <= this.desiredKeyError) {
            return true;
        }
        return false;
    }
}
