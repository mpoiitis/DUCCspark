package com.poiitis.graphUtils;

import com.poiitis.columns.ColumnCombinationBitset;
import com.poiitis.exceptions.ColumnNameMismatchException;
import com.poiitis.exceptions.CouldNotReceiveResultException;
import com.poiitis.pli.PositionListIndex;
import com.poiitis.utils.Singleton;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *
 * @author Marinos Poiitis
 */
public abstract class GraphTraverser implements Serializable{

    private static final long serialVersionUID = -6985441634862426686L;
    protected int OVERFLOW_THRESHOLD = 10000;
    protected int PLI_SEARCH_THRESHOLD = 1000;
    protected JavaPairRDD<ColumnCombinationBitset, PositionListIndex> calculatedPlis;
    protected List<ColumnCombinationBitset> calculatedPliBitsetStack = new LinkedList<ColumnCombinationBitset>();
    protected Broadcast<ColumnCombinationBitset> bitmaskForNonUniqueColumns;
    protected int numberOfColumns;
    protected PruningGraph negativeGraph;
    protected PruningGraph positiveGraph;
    protected JavaRDD<ColumnCombinationBitset> minimalPositives;
    protected JavaRDD<ColumnCombinationBitset> maximalNegatives;
    protected Deque<ColumnCombinationBitset> randomWalkTrace = new LinkedList<ColumnCombinationBitset>();
    protected JavaRDD<ColumnCombinationBitset> seedCandidates;
    protected HoleFinder holeFinder;
    protected Random random = new Random();
    protected int found;
    
    /**
     * main traversal method
     */
    public int traverseGraph() throws CouldNotReceiveResultException, ColumnNameMismatchException {
        this.found = 0;
        ColumnCombinationBitset currentColumn = this.getSeed();
        while (null != currentColumn) {
            this.randomWalk(currentColumn);
            currentColumn = this.getSeed();
        }
        return this.found;
    }
    
    /**
     * random walk traversal implementation
     */
    protected void randomWalk(ColumnCombinationBitset currentColumnCombination) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        while (null != currentColumnCombination) {
            ColumnCombinationBitset newColumn;
            if (this.isSubsetOfMaximalNegativeColumnCombination(currentColumnCombination)) {
                newColumn = null;
            } else if (this.isSupersetOfPositiveColumnCombination(currentColumnCombination)) {
                newColumn = null;
            } else if (this.isPositiveColumnCombination(currentColumnCombination)) {
                newColumn = this.getNextChildColumnCombination(currentColumnCombination);
                if (null == newColumn) {
                    this.addMinimalPositive(currentColumnCombination);
                }
                this.positiveGraph.add(currentColumnCombination);
            } else {
                newColumn = this.getNextParentColumnCombination(currentColumnCombination);
                if (null == newColumn) {
                    this.addMaximalNegatives(currentColumnCombination);
                    this.holeFinder.update(currentColumnCombination);
                }
                this.negativeGraph.add(currentColumnCombination);
            }
            if (null != newColumn) {
                this.randomWalkTrace.push(currentColumnCombination);
                currentColumnCombination = newColumn;
                continue;
            }
            if (this.randomWalkTrace.isEmpty()) {
                return;
            }
            currentColumnCombination = this.randomWalkTrace.poll();
        }
    }
    
    protected abstract boolean isPositiveColumnCombination(ColumnCombinationBitset var1);
    
    protected ColumnCombinationBitset getSeed() {
        ColumnCombinationBitset seedCandidate = this.findUnprunedSetAndUpdateGivenList(this.seedCandidates, true);
        if (seedCandidate == null) {
            this.seedCandidates = this.getHoles();
            seedCandidate = this.findUnprunedSetAndUpdateGivenList(this.seedCandidates, true);
        }
        return seedCandidate;
    }
    
    protected JavaRDD<ColumnCombinationBitset> getHoles() {
        return this.holeFinder.getHolesWithoutGivenColumnCombinations(this.minimalPositives);
    }
    
    protected PositionListIndex getPLIFor(ColumnCombinationBitset columnCombination) {
        
        PositionListIndex pli = this.getPli(columnCombination);
        
        if (pli != null) {
            return pli;
        }
        pli = this.createPliFromExistingPli(columnCombination);
        return pli;
    }
    
    /**
     * Get a pli according to a specific column combination bitset
     */
    protected PositionListIndex getPli(ColumnCombinationBitset ccb){
        PositionListIndex pli = this.calculatedPlis.filter((Tuple2<ColumnCombinationBitset,
            PositionListIndex> tuple) -> tuple._1.equals(ccb)).map((Tuple2<ColumnCombinationBitset,
                PositionListIndex> tuple) -> tuple._2).first();
        
        return pli;
    }
    
    protected PositionListIndex createPliFromExistingPli(ColumnCombinationBitset columnCombination) {
        int counter = 0;
        ColumnCombinationBitset currentBestSet = (ColumnCombinationBitset)columnCombination.getContainedOneColumnCombinations().get(0);
        ColumnCombinationBitset currentBestMinusSet = columnCombination.minus(currentBestSet);
        Iterator<ColumnCombinationBitset> itr = this.calculatedPliBitsetStack.iterator();
        while (itr.hasNext() && counter < this.PLI_SEARCH_THRESHOLD) {
            ColumnCombinationBitset currentSet = itr.next();
            if (currentSet.size() >= columnCombination.size()) continue;
            ++counter;
            if (!currentSet.isSubsetOf(columnCombination)) continue;
            ColumnCombinationBitset currentMinusSet = columnCombination.minus(currentSet);
            PositionListIndex currentMinusPli = this.getPli(currentMinusSet);
            if (currentMinusPli != null) {
                PositionListIndex intersect = this.getPli(currentSet).intersect(currentMinusPli);
                this.addPli(columnCombination, intersect);
                return intersect;
            }
            if (currentBestSet.size() >= currentSet.size()) continue;
            currentBestSet = currentSet;
            currentBestMinusSet = currentMinusSet;
        }
        return this.extendPli(currentBestSet, currentBestMinusSet);
    }

    
    protected PositionListIndex extendPli(ColumnCombinationBitset columnCombination, ColumnCombinationBitset extendingColumns) {
        PositionListIndex currentPli = this.getPli(columnCombination);
        for (ColumnCombinationBitset currentOneColumnCombination : extendingColumns.getContainedOneColumnCombinations()) {
            currentPli = currentPli.intersect(this.getPli(currentOneColumnCombination));
            columnCombination = columnCombination.union(currentOneColumnCombination);
            this.addPli(columnCombination, currentPli);
        }
        return currentPli;
    }

    protected void addPli(ColumnCombinationBitset columnCombination, PositionListIndex pli) {
        
        List<Tuple2<ColumnCombinationBitset, PositionListIndex>> list = new ArrayList<>();
        list.add(new Tuple2<>(columnCombination, pli));
        
        //create pli of the proper form in order to unify with calculatedPlis
        JavaRDD<Tuple2<ColumnCombinationBitset, PositionListIndex>> rdd = Singleton.getSparkContext().parallelize(list);
        JavaPairRDD<ColumnCombinationBitset, PositionListIndex> pairRdd = rdd.mapToPair((Tuple2<ColumnCombinationBitset,
                PositionListIndex> tuple) -> tuple);
        
        this.calculatedPlis.union(pairRdd);
        this.calculatedPliBitsetStack.add(0, columnCombination);
    }
    
    public JavaPairRDD<ColumnCombinationBitset, PositionListIndex> getCalculatedPlis() {
        return this.calculatedPlis;
    }
    
    protected ColumnCombinationBitset getNextParentColumnCombination(ColumnCombinationBitset column) {
        //if minimal positives contain column return null
        if(!this.minimalPositives.filter((ColumnCombinationBitset ccb) -> ccb.equals(column)).isEmpty()){
            return null;
        }
        List supersets = column.getDirectSupersets(this.bitmaskForNonUniqueColumns.value());
        JavaRDD<ColumnCombinationBitset> supersetsRdd = Singleton.getSparkContext().parallelize(supersets);
        
        return this.findUnprunedSet(supersetsRdd);
    }

    protected ColumnCombinationBitset getNextChildColumnCombination(ColumnCombinationBitset column) {
        if (column.size() == 1) {
            return null;
        }
        //if maximal negatives contain column return null
        if(!this.maximalNegatives.filter((ColumnCombinationBitset ccb) -> ccb.equals(column)).isEmpty()){
            return null;
        }
        
        List subsets = column.getDirectSubsets();
        JavaRDD<ColumnCombinationBitset> subsetsRdd = Singleton.getSparkContext().parallelize(subsets);
        return this.findUnprunedSet(subsetsRdd);
    }
    
    protected ColumnCombinationBitset findUnprunedSet(JavaRDD<ColumnCombinationBitset> sets) {
        return this.findUnprunedSetAndUpdateGivenList(sets, false);
    }
    
    protected ColumnCombinationBitset findUnprunedSetAndUpdateGivenList(JavaRDD<ColumnCombinationBitset> setsRdd, boolean setPrunedEntriesToNull) {
        if (setsRdd.isEmpty()) {
            return null;
        }
        List<ColumnCombinationBitset> sets = setsRdd.collect();
        int random = this.random.nextInt(sets.size());
        for (int i = 0; i < sets.size(); ++i) {
            int no = (i + random) % sets.size();
            ColumnCombinationBitset singleSet = sets.get(no);
            if (singleSet == null) continue;
            if (this.isAdditionalConditionTrueForFindUnprunedSetAndUpdateGivenList(singleSet)) {
                if (!setPrunedEntriesToNull) continue;
                sets.set(no, null);
                continue;
            }
            if (this.positiveGraph.find(singleSet)) {
                if (!setPrunedEntriesToNull) continue;
                sets.set(no, null);
                continue;
            }
            if (this.negativeGraph.find(singleSet)) {
                if (!setPrunedEntriesToNull) continue;
                sets.set(no, null);
                continue;
            }
            return singleSet;
        }
        return null;
    }

    /**
     * fill an rdd with the column combinations that are subsets of argument.
     * Return true if there is at least one combination.
     */
    protected boolean isSupersetOfPositiveColumnCombination(ColumnCombinationBitset currentColumnCombination) {
        return !this.minimalPositives.filter((ColumnCombinationBitset ccb) -> ccb.isSubsetOf(currentColumnCombination)).isEmpty();
    }

    /**
     * fill an rdd with the column combinations that contain argument as subset.
     * Return true if there is at least one combination.
     */
    protected boolean isSubsetOfMaximalNegativeColumnCombination(ColumnCombinationBitset currentColumnCombination) {   
        return !this.maximalNegatives.filter((ColumnCombinationBitset ccb) -> ccb.containsSubset(currentColumnCombination)).isEmpty();
    }
    
    /**
     * Add a column combination to minimal positives
     */
    protected void addMinimalPositive(ColumnCombinationBitset positiveColumnCombination) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        List<ColumnCombinationBitset> list = new ArrayList<>();
        list.add(positiveColumnCombination);
        JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(list);
        
        this.minimalPositives.union(rdd);
        ++this.found;
    }
    
    
    /**
     * Add a column combination to maximal negatives 
     */
    protected void addMaximalNegatives(ColumnCombinationBitset ccb) throws CouldNotReceiveResultException, ColumnNameMismatchException{
        List<ColumnCombinationBitset> list = new ArrayList<>();
        list.add(ccb);
        JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(list);
        this.maximalNegatives.union(rdd);
    }    

    public JavaRDD<ColumnCombinationBitset> getMinimalPositiveColumnCombinations() {
        return this.minimalPositives;
    }

    protected abstract boolean isAdditionalConditionTrueForFindUnprunedSetAndUpdateGivenList(ColumnCombinationBitset var1);
    
}

