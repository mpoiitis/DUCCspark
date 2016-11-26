package com.poiitis.graphUtils;

import com.poiitis.columns.ColumnCombinationBitset;
import com.poiitis.exceptions.ColumnNameMismatchException;
import com.poiitis.exceptions.CouldNotReceiveResultException;
import com.poiitis.pli.PositionListIndex;
import com.poiitis.utils.Singleton;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
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
    protected Set<ColumnCombinationBitset> maximalNegatives = new HashSet<ColumnCombinationBitset>();
    protected Deque<ColumnCombinationBitset> randomWalkTrace = new LinkedList<ColumnCombinationBitset>();
    protected List<ColumnCombinationBitset> seedCandidates;
    protected HoleFinder holeFinder;
    protected Random random = new Random();
    protected int found;


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
    
}
