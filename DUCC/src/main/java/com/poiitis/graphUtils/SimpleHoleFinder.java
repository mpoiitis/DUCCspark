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
public class SimpleHoleFinder implements Serializable{

    private static final long serialVersionUID = -3893528003163066611L;

    protected JavaRDD<ColumnCombinationBitset> holes;
    protected ColumnCombinationBitset allBitsSet = new ColumnCombinationBitset(new int[0]);

    public SimpleHoleFinder(int numberOfColumns) {
        this.allBitsSet.setAllBits(numberOfColumns);
        this.holes = Singleton.getSparkContext().emptyRDD();
    }

    public JavaRDD<ColumnCombinationBitset> getHoles() {
        this.holes = this.holes.cache();
        return this.holes;
    }
    
    public void clearHoles(){
        this.holes = this.holes.unpersist();
        this.holes = Singleton.getSparkContext().emptyRDD();
    }

    /**
     * Take complement of maximal negative. Add it in holes if it is not contained in minimal positives.
     * @param maximalNegative
     * @param minimalPositives 
     */
    public void update(ColumnCombinationBitset maximalNegative, JavaRDD<ColumnCombinationBitset> minimalPositives) {
        
        ColumnCombinationBitset complementary = this.allBitsSet.minus(maximalNegative);
        
        if(complementary.isEmpty()) return;
        
        if (minimalPositives.isEmpty()) {
            //add it directly to holes
            List<ColumnCombinationBitset> list = new ArrayList<>();
            list.add(complementary);
            JavaRDD<ColumnCombinationBitset> unionRdd = Singleton.getSparkContext().parallelize(list);
            this.holes = this.holes.union(unionRdd);
        } else {
            //check if complementary is contained in minimal positives
            Broadcast<ColumnCombinationBitset> bComplementary = Singleton.getSparkContext().broadcast(complementary);
            JavaRDD<ColumnCombinationBitset> rdd = minimalPositives.filter((ColumnCombinationBitset c) -> c.equals((Object) bComplementary.value()));

            //if it is not contained add it to holes
            if (rdd.isEmpty()) {
                List<ColumnCombinationBitset> list = new ArrayList<>();
                list.add(complementary);
                JavaRDD<ColumnCombinationBitset> unionRdd = Singleton.getSparkContext().parallelize(list);
                this.holes = this.holes.union(unionRdd);
            }
        }
    }

}
