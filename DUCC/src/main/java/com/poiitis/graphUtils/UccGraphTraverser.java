package com.poiitis.graphUtils;

import com.poiitis.columns.ColumnCombinationBitset;
import com.poiitis.exceptions.ColumnNameMismatchException;
import com.poiitis.exceptions.CouldNotReceiveResultException;
import com.poiitis.pli.PositionListIndex;
import java.io.Serializable;
import java.util.List;
import java.util.Random;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *
 * @author Marinos Poiitis
 */
public class UccGraphTraverser
extends GraphTraverser implements Serializable {

    private static final long serialVersionUID = -1252806705177680171L;
    protected JavaRDD<ColumnCombinationBitset> results;
    protected List<Tuple2<String,Integer>> columnNames;
    protected long desiredKeyError = 0;

    public UccGraphTraverser() {

    }

    public UccGraphTraverser(Random random) {
        this();
        this.random = random;
    }

    public void setDesiredKeyError(long desiredKeyError) {
        this.desiredKeyError = desiredKeyError;
    }

    public void init(JavaRDD<PositionListIndex> basePLIs, List<Tuple2<String,Integer>> columnNames,
            JavaSparkContext sc) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        this.columnNames = columnNames;
        this.filterNonUniqueColumnCombinationBitsets(basePLIs, sc);
    }

    protected void filterNonUniqueColumnCombinationBitsets(JavaRDD<PositionListIndex> basePLIs,
            JavaSparkContext sc) throws CouldNotReceiveResultException, ColumnNameMismatchException {
       
        ColumnCombinationBitset temp = new ColumnCombinationBitset(new int[0]);
        //create broadcast variable so as to change in each pli
        this.bitmaskForNonUniqueColumns = sc.broadcast(temp);
        
        this.calculatedPlis = basePLIs.mapToPair((PositionListIndex pli) -> {
            //there is always a single column in pli as it is basepli
            Tuple2<String,Integer> tuple = pli.getName().get(0);
            ColumnCombinationBitset currentColumnCombination = new ColumnCombinationBitset(new int[]{tuple._2});
            
            return new Tuple2<>(currentColumnCombination, pli);
        });
        
        this.calculatedPlis.cache();
        
        //keep only unique plis and add them to minimal graph
        this.minimalPositives = this.calculatedPlis.filter(new Function<Tuple2<ColumnCombinationBitset, 
                PositionListIndex>, Boolean>() {
            @Override
            public Boolean call(Tuple2<ColumnCombinationBitset,
                    PositionListIndex> tuple) throws Exception {
                return isUnique(tuple._2);
            }
        }).map(new Function<Tuple2<ColumnCombinationBitset, PositionListIndex>, ColumnCombinationBitset>() {
            @Override
            public ColumnCombinationBitset call(Tuple2<ColumnCombinationBitset,
                    PositionListIndex> tuple) throws Exception {
                return tuple._1;
            }
        });
        
        //put all found unique column combinations in results
        this.results = this.minimalPositives;
        System.out.println(this.results.collect().size());
        //take all non-unique plis and create the bitmask for them
        this.calculatedPlis.filter((Tuple2<ColumnCombinationBitset,
            PositionListIndex> tuple) -> (!isUnique(tuple._2))).foreach((Tuple2<ColumnCombinationBitset,
                PositionListIndex> tuple) -> {
            //again we know that pli consists of a single column, thats why get(0)
            int columnIndex = tuple._2.getName().get(0)._2;
            System.out.println(bitmaskForNonUniqueColumns.value());
            bitmaskForNonUniqueColumns.value().addColumn(columnIndex);
        });
            
        System.out.println("=======================================");
        System.out.println(this.bitmaskForNonUniqueColumns.value().toString());
                
        /*for (PositionListIndex pli : basePLIs) {
            ColumnCombinationBitset currentColumnCombination = new ColumnCombinationBitset(new int[]{columnIndex});
            this.calculatedPlis.put(currentColumnCombination, pli);
            if (this.isUnique(pli)) {
                this.minimalPositives.add(currentColumnCombination);
                this.resultReceiver.receiveResult(new UniqueColumnCombination(new ColumnIdentifier[]{new ColumnIdentifier(this.relationName, this.columnNames.get(columnIndex))}));
            } else {
                this.bitmaskForNonUniqueColumns.addColumn(columnIndex);
            }
            ++columnIndex;
        }*/
    }

    protected List<ColumnCombinationBitset> buildInitialSeeds() {
        return this.bitmaskForNonUniqueColumns.value().getNSubsetColumnCombinations(2);
    }

    /*protected boolean isPositiveColumnCombination(ColumnCombinationBitset currentColumnCombination) {
        return this.isUnique(this.getPLIFor(currentColumnCombination));
    }*/

    protected void addMinimalPositive(ColumnCombinationBitset positiveColumnCombination) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        /*this.minimalPositives.add(positiveColumnCombination);
        this.resultReceiver.receiveResult(new UniqueColumnCombination(positiveColumnCombination.createColumnCombination(this.relationName, this.columnNames)));
        */
    }

    protected boolean isAdditionalConditionTrueForFindUnprunedSetAndUpdateGivenList(ColumnCombinationBitset singleSet) {
        return false;
    }

    protected boolean isUnique(PositionListIndex pli) {
        if (this.desiredKeyError == 0) {
            return pli.isUnique();
        }
        if (pli.getRawKeyError() <= this.desiredKeyError) {
            return true;
        }
        return false;
    }
}
