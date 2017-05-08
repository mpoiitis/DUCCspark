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
public abstract class SimpleGraphTraverser implements Serializable{

    private static final long serialVersionUID = -6985441634862426686L;
    protected int OVERFLOW_THRESHOLD = 10000;
    protected int PLI_SEARCH_THRESHOLD = 1000;
    protected JavaPairRDD<ColumnCombinationBitset, PositionListIndex> calculatedPlis;
    protected List<ColumnCombinationBitset> calculatedPliBitsetStack = new LinkedList<ColumnCombinationBitset>();
    protected ColumnCombinationBitset bitmaskForNonUniqueColumns = new ColumnCombinationBitset();
    protected int numberOfColumns;
    protected SimplePruningGraph negativeGraph;
    protected SimplePruningGraph positiveGraph;
    protected JavaRDD<ColumnCombinationBitset> minimalPositives = Singleton.getSparkContext().emptyRDD();
    protected JavaRDD<ColumnCombinationBitset> maximalNegatives = Singleton.getSparkContext().emptyRDD();
    protected Deque<ColumnCombinationBitset> randomWalkTrace = new LinkedList<ColumnCombinationBitset>();
    protected JavaRDD<ColumnCombinationBitset> seedCandidates;
    protected SimpleHoleFinder holeFinder;
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
            /*System.out.println("Marinos Random walk trace: ");
            for(Iterator itr = this.randomWalkTrace.iterator();itr.hasNext();)  {
                System.out.println("Marinos " + itr.next());
            }*/
            
            //System.out.println("Marinos Random walk trace =============");
            ColumnCombinationBitset newColumn = null;
            if (this.isSubsetOfMaximalNegativeColumnCombination(currentColumnCombination)) {
                //System.out.println("Marinos Subset of maximal negative " + currentColumnCombination);
                //System.out.println("Marinos newColumn = NULL");
                newColumn = null;
            } else if (this.isSupersetOfPositiveColumnCombination(currentColumnCombination)) {
                //System.out.println("Marinos Superset of positive " + currentColumnCombination);
                //System.out.println("Marinos newColumn = NULL");
                newColumn = null;
            } else if (this.isPositiveColumnCombination(currentColumnCombination)) {
                //System.out.println("Marinos Is positive " + currentColumnCombination);
                this.positiveGraph.add(currentColumnCombination);
                newColumn = this.getNextChildColumnCombination(currentColumnCombination);
                
                /*
                if(null == newColumn){
                    System.out.println("Marinos newColumn = NULL");
                }
                else{
                    System.out.println("Marinos newColumn = " + newColumn);
                }*/
                
                if (null == newColumn) {
                    //System.out.println("Marinos Add minimal positive " + currentColumnCombination);
                    this.addMinimalPositive(currentColumnCombination);
                }               
            } else if (!this.isPositiveColumnCombination(currentColumnCombination)){
                //System.out.println("Marinos Is not positive " + currentColumnCombination);
                this.negativeGraph.add(currentColumnCombination);
                newColumn = this.getNextParentColumnCombination(currentColumnCombination);
                
                /*if(null == newColumn){
                    System.out.println("Marinos newColumn = NULL");
                }
                else{
                    System.out.println("Marinos newColumn = " + newColumn);
                }*/
                
                if (null == newColumn) {
                    //System.out.println("Marinos Add maximal negative " + currentColumnCombination);
                    this.addMaximalNegatives(currentColumnCombination);
                    this.holeFinder.update(currentColumnCombination, this.minimalPositives);
                }
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

        if (seedCandidate == null) {;
            this.seedCandidates = this.getHoles();
            seedCandidate = this.findUnprunedSetAndUpdateGivenList(this.seedCandidates, true);
        }
        //System.out.println("Marinos Get Seed: " + seedCandidate);
        return seedCandidate;
    }
    
    /*protected JavaRDD<ColumnCombinationBitset> getHoles() {
        return this.holeFinder.getHolesWithoutGivenColumnCombinations(this.minimalPositives);
    }*/
    protected JavaRDD<ColumnCombinationBitset> getHoles() {
        JavaRDD<ColumnCombinationBitset> rdd = this.holeFinder.getHoles();
        this.holeFinder.clearHoles();
        return rdd;
    }
    
    protected PositionListIndex getPLIFor(ColumnCombinationBitset columnCombination) {
        PositionListIndex pli = this.getPli(columnCombination);
        
        if (pli != null) {
            return pli;
        }
        
        pli = this.createPliFromExistingPli(columnCombination);
        
        if(pli == null){
            return new PositionListIndex();
        }
        else{
            return pli;
        }
    }
    
    /**
     * Get a pli according to a specific column combination bitset
     */
    protected PositionListIndex getPli(ColumnCombinationBitset ccb){

        Broadcast<ColumnCombinationBitset> bCcb = Singleton.getSparkContext().broadcast(ccb);//so as to be used in filter

        JavaRDD<PositionListIndex> filtered = this.calculatedPlis.filter((Tuple2<ColumnCombinationBitset,
            PositionListIndex> tuple) -> {
            return tuple._1.equals(bCcb.value());
        }).map((Tuple2<ColumnCombinationBitset, PositionListIndex> tuple) -> tuple._2);
        
        PositionListIndex pli = filtered.isEmpty() ? null : filtered.first();
        
        //destroy created broadcast variable
        bCcb.destroy();
        
        return pli;
    }
    
    protected PositionListIndex createPliFromExistingPli(ColumnCombinationBitset columnCombination) {
        int counter = 0;
        
        List<ColumnCombinationBitset> containedOneColumnCombinations = columnCombination.getContainedOneColumnCombinations();
        ColumnCombinationBitset currentBestSet;
        if(containedOneColumnCombinations.isEmpty()){
            currentBestSet = new ColumnCombinationBitset();
        }
        else{
            currentBestSet = containedOneColumnCombinations.get(0);
        }
        
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
            PositionListIndex> tuple) -> {
            return tuple;
        });

        this.calculatedPlis = this.calculatedPlis.union(pairRdd);
        this.calculatedPliBitsetStack.add(0, columnCombination);
    }
    
    public JavaPairRDD<ColumnCombinationBitset, PositionListIndex> getCalculatedPlis() {
        return this.calculatedPlis;
    }
    
    protected ColumnCombinationBitset getNextParentColumnCombination(ColumnCombinationBitset column) {
        
        Broadcast<ColumnCombinationBitset> bColumn = Singleton.getSparkContext().broadcast(column);
       
        //if minimal positives contain column return null
        if(!this.minimalPositives.filter((ColumnCombinationBitset ccb) -> ccb.equals(bColumn.value())).isEmpty()){
            return null;
        }
        List<ColumnCombinationBitset> supersets = column.getDirectSupersets(this.bitmaskForNonUniqueColumns);
        JavaRDD<ColumnCombinationBitset> supersetsRdd = Singleton.getSparkContext().parallelize(supersets);
        
        //destroy broadcast variable
        bColumn.destroy();
        
        return this.findUnprunedSet(supersetsRdd);
    }

    protected ColumnCombinationBitset getNextChildColumnCombination(ColumnCombinationBitset column) {
        if (column.size() == 1) {
            return null;
        }
        Broadcast<ColumnCombinationBitset> bColumn = Singleton.getSparkContext().broadcast(column);
        
        //if maximal negatives contain column return null
        if(!this.maximalNegatives.filter((ColumnCombinationBitset ccb) -> ccb.equals(bColumn.value())).isEmpty()){
            return null;
        }
        
        List<ColumnCombinationBitset> subsets = column.getDirectSubsets();
        JavaRDD<ColumnCombinationBitset> subsetsRdd = Singleton.getSparkContext().parallelize(subsets);
        
        //destroy broadcast variable
        bColumn.destroy();
        
        return this.findUnprunedSet(subsetsRdd);
    }
    
    protected ColumnCombinationBitset findUnprunedSet(JavaRDD<ColumnCombinationBitset> sets) {
        return this.findUnprunedSetAndUpdateGivenList(sets, false);
    }
    
    protected ColumnCombinationBitset findUnprunedSetAndUpdateGivenList(JavaRDD<ColumnCombinationBitset> setsRdd, boolean setPrunedEntriesToNull) {
        if (setsRdd.isEmpty()) {
            return null;
        }

        List<ColumnCombinationBitset> sets = new ArrayList<>(setsRdd.collect());

        int random = this.random.nextInt(sets.size());
        for (int i = 0; i < sets.size(); ++i) {
            int no = (i + random) % sets.size();
            ColumnCombinationBitset singleSet = sets.get(no);
            
            if (singleSet == null){
                continue;
            }
            if (this.isAdditionalConditionTrueForFindUnprunedSetAndUpdateGivenList(singleSet)) {
                if (!setPrunedEntriesToNull) continue;
                sets.set(no, null);
                continue;
            }
            if (this.positiveGraph.find(singleSet)) {
                //System.out.println("Marinos found positive " + singleSet);
                if (!setPrunedEntriesToNull) continue;
                sets.set(no, null);
                continue;
            }
            if (this.negativeGraph.find(singleSet)) {
                //System.out.println("Marinos found negative " + singleSet);
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
        Broadcast<ColumnCombinationBitset> bCurrentColumnCombination = Singleton.getSparkContext().broadcast(currentColumnCombination);
        long c1 =this.minimalPositives.count();
        Boolean b = !this.minimalPositives.filter((ColumnCombinationBitset ccb) -> ccb.isSubsetOf(bCurrentColumnCombination.value())).isEmpty();
        return b;
    }

    /**
     * fill an rdd with the column combinations that contain argument as subset.
     * Return true if there is at least one combination.
     */
    protected boolean isSubsetOfMaximalNegativeColumnCombination(ColumnCombinationBitset currentColumnCombination) {   
        Broadcast<ColumnCombinationBitset> bCurrentColumnCombination = Singleton.getSparkContext().broadcast(currentColumnCombination);   
        return !this.maximalNegatives.filter((ColumnCombinationBitset ccb) -> ccb.containsSubset(bCurrentColumnCombination.value())).isEmpty();
    }
    
    /**
     * Add a column combination to minimal positives
     */
    protected void addMinimalPositive(ColumnCombinationBitset ccb) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        
        List<ColumnCombinationBitset> list = new ArrayList<>();
        list.add(ccb);
        JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(list);
        
        this.minimalPositives = this.minimalPositives.union(rdd);
        this.minimalPositives = this.minimalPositives.cache();
        ++this.found;
    }
    
    
    /**
     * Add a column combination to maximal negatives 
     */
    protected void addMaximalNegatives(ColumnCombinationBitset ccb) throws CouldNotReceiveResultException, ColumnNameMismatchException{

        List<ColumnCombinationBitset> list = new ArrayList<>();
        list.add(ccb);
        JavaRDD<ColumnCombinationBitset> rdd = Singleton.getSparkContext().parallelize(list);

        this.maximalNegatives = this.maximalNegatives.union(rdd);
        this.maximalNegatives = this.maximalNegatives.cache();
    }

    /**
     * check if given column combination is minimal unique
     */
    protected boolean isMinimalPositive(ColumnCombinationBitset ccb){
        
        Broadcast<ColumnCombinationBitset> bCcb = Singleton.getSparkContext().broadcast(ccb);
        JavaRDD<ColumnCombinationBitset> rdd = this.minimalPositives.filter((ColumnCombinationBitset c) ->{
            return c.equals(bCcb.value());
        });
        
        return !rdd.isEmpty();
    }
    
    /**
     * check if given column combination is maximal non unique
     */
    protected boolean isMaximalNegative(ColumnCombinationBitset ccb){
        
        Broadcast<ColumnCombinationBitset> bCcb = Singleton.getSparkContext().broadcast(ccb);
        JavaRDD<ColumnCombinationBitset> rdd = this.maximalNegatives.filter((ColumnCombinationBitset c) ->{
            return c.equals(bCcb.value());
        });
        
        return !rdd.isEmpty();
    }

    public JavaRDD<ColumnCombinationBitset> getMinimalPositiveColumnCombinations() {
        return this.minimalPositives;
    }

    protected abstract boolean isAdditionalConditionTrueForFindUnprunedSetAndUpdateGivenList(ColumnCombinationBitset var1);
    
}

