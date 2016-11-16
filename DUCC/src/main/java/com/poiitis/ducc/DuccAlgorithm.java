package com.poiitis.ducc;

import com.google.common.collect.ImmutableList;
import com.poiitis.columns.ColumnCombinationBitset;
import com.poiitis.exceptions.AlgorithmExecutionException;
import com.poiitis.graphUtils.UccGraphTraverser;
import com.poiitis.pli.PositionListIndex;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 *
 * @author Marinos Poiitis
 */
public class DuccAlgorithm {
    
    public int found;
    protected List<Tuple2<String,Integer>> columnNames;
    protected UccGraphTraverser graphTraverser;
    protected long desiredRawKeyError = 0;
    
    public DuccAlgorithm( List<Tuple2<String,Integer>> columnNames) {
        this.columnNames = columnNames;
        this.graphTraverser = new UccGraphTraverser();
    }

    public DuccAlgorithm(List<Tuple2<String,Integer>> columnNames, Random random) {
        this(columnNames);
        this.graphTraverser = new UccGraphTraverser(random);
    }
    
    public void setRawKeyError(long keyError) {
        this.desiredRawKeyError = keyError;
        this.graphTraverser.setDesiredKeyError(keyError);
    }

    public void run(JavaRDD<PositionListIndex> pliList) throws AlgorithmExecutionException {
        this.found = 0;
        this.graphTraverser.init(pliList,this.columnNames);
        //this.found = this.graphTraverser.traverseGraph();
    }
/*
    public ImmutableList<ColumnCombinationBitset> getMinimalUniqueColumnCombinations() {
        return ImmutableList.copyOf((Collection)this.graphTraverser.getMinimalPositiveColumnCombinations());
    }

    public Map<ColumnCombinationBitset, PositionListIndex> getCalculatedPlis() {
        return this.graphTraverser.getCalculatedPlis();
    }
*/
}
