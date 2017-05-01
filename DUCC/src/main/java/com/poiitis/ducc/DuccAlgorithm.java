package com.poiitis.ducc;

import com.google.common.collect.ImmutableList;
import com.poiitis.columns.ColumnCombinationBitset;
import com.poiitis.exceptions.AlgorithmExecutionException;
import com.poiitis.graphUtils.SimpleUccGraphTraverser;
import com.poiitis.pli.PositionListIndex;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 *
 * @author Marinos Poiitis
 */
public class DuccAlgorithm implements Serializable{

    private static final long serialVersionUID = 3207372627237038350L;
    
    public int found;
    protected List<Tuple2<String,Integer>> columnNames;

    protected SimpleUccGraphTraverser graphTraverser;
    protected long desiredRawKeyError = 0;

    
    public DuccAlgorithm(List<Tuple2<String,Integer>> columnNames) {

        this.columnNames = columnNames;

        this.graphTraverser = new SimpleUccGraphTraverser();
    }

    public DuccAlgorithm(List<Tuple2<String,Integer>> columnNames, Random random) {
        this(columnNames);

        this.graphTraverser = new SimpleUccGraphTraverser(random);
    }
    
    public void setRawKeyError(long keyError) {
        this.desiredRawKeyError = keyError;
        this.graphTraverser.setDesiredKeyError(keyError);
    }

    public void run(JavaRDD<PositionListIndex> pliList) throws AlgorithmExecutionException {
        this.found = 0;
        this.graphTraverser.init(pliList, this.columnNames);
        this.found = this.graphTraverser.traverseGraph();
        //this.graphTraverser.saveResults("hdfs://localhost:9000/user/mpoiitis/output");
        this.graphTraverser.saveResults("output");
    }

    public ImmutableList<ColumnCombinationBitset> getMinimalUniqueColumnCombinations() {
        return ImmutableList.copyOf((Collection)this.graphTraverser.getMinimalPositiveColumnCombinations().collect());
    }
}
