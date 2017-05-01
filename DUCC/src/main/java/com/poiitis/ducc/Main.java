package com.poiitis.ducc;

import com.poiitis.exceptions.AlgorithmExecutionException;
import com.poiitis.pli.PLIBuilder;
import com.poiitis.pli.PositionListIndex;
import com.poiitis.utils.Singleton;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.api.java.JavaRDD;


/**
 *
 * @author Poiitis Marinos
 */
public class Main implements Serializable{
    
    //private static final String INPUT_PATH = "hdfs://localhost:9000/user/mpoiitis/input"; 
    //private static final String CHECKPOINT_PATH = "hdfs://localhost:9000/user/mpoiitis/checkpoint"; 
    private static final String INPUT_PATH = "input";
    //private static final String CHECKPOINT_PATH = "hdfs:///user/mpoiitis/checkpoint"; 
    
    private static final long serialVersionUID = 6577720769633116725L;
    
    public static void main(String[] args){
        
        int numPartitions = 4;
        
        //set checkpoint path
        //Singleton.getSparkContext().setCheckpointDir(CHECKPOINT_PATH);
        
        JavaRDD<String> input = Singleton.getSparkContext().textFile(INPUT_PATH, numPartitions);
        
        //CustomParser parser = new CustomParser(input);
        Parser parser = new Parser(input);
        JavaRDD<Adult> adults = parser.parseFile();
        
        PLIBuilder pliBuilder = new PLIBuilder(adults, true);
        pliBuilder.createInitialPLIs();
        JavaRDD<PositionListIndex> plis = pliBuilder.getPLIList();

        DuccAlgorithm duccAlgorithm = new DuccAlgorithm(parser.getColumnNames());
        try {
            duccAlgorithm.run(plis);
        } catch (AlgorithmExecutionException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
}
