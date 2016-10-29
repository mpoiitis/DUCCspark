package com.poiitis.pli;

import com.poiitis.ducc.Adult;
import com.poiitis.exceptions.InputIterationException;
import it.unimi.dsi.fastutil.Hash;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author Poiitis Marinos
 */
public class PLIBuilder implements Serializable{

    private static final long serialVersionUID = 4997530231064808611L;

    protected long numberOfTuples = -1;
    protected boolean nullEqualsNull;
    protected JavaRDD<Adult> adults;

    public PLIBuilder(JavaRDD<Adult> adults) {
        this.adults = adults;
        //adults.persist(StorageLevel.MEMORY_AND_DISK_SER());
        this.nullEqualsNull = true;
    }

    public PLIBuilder(JavaRDD<Adult> adults, boolean nullEqualsNull) {
        this.adults = adults;
        //adults.persist(StorageLevel.MEMORY_AND_DISK_SER());
        this.nullEqualsNull = nullEqualsNull;
    }

    /**
     * Returns the number of tuples in the input after calculating the plis. Can
     * be used after calculateUnpurgedPLI was called.
     *
     * @return number of tuples in dataset
     * @throws input.InputIterationException if the number of tuples is less or
     * equal to zero
     */
    public long getNumberOfTuples() throws InputIterationException {
        if (this.numberOfTuples == -1) {
            throw new InputIterationException();
        } else {
            return this.numberOfTuples;
        }
    }

    public void createInitialPLIs() {

        //calculate number of rows in dataset
        JavaPairRDD<String, Long> count = adults.mapToPair((Adult x) -> new 
            Tuple2("dummyKey", (long)1)).reduceByKey(new Function2<Long, Long, Long>(){
            public Long call(Long x, Long y){ return x + y;}});

        this.numberOfTuples = count.first()._2;
        
        //list of all items of all adults in form col,(row,cellValue)
        JavaPairRDD<String, Tuple2<Long,String>> prePlis = adults.flatMapToPair(
                new PairFlatMapFunction<Adult, String, Tuple2<Long, String>>() {
            public Iterator<Tuple2<String, Tuple2<Long, String>>> call(Adult adult) 
                    throws Exception {
                ArrayList<Tuple2<String, Tuple2<Long, String>>> result = new 
                        ArrayList<>();
                LinkedHashMap<String, String> lhmap = adult.getAttributes();
                for(Map.Entry<String,String> entry : lhmap.entrySet()){
                    Tuple2<Long, String> innerTuple = new Tuple2<>(adult.getLineNumber(), 
                    entry.getValue());
                    Tuple2<String, Tuple2<Long, String>> tuple = 
                            new Tuple2<>(entry.getKey(), innerTuple);   
                    
                    result.add(tuple);
                }
                
                return result.iterator();
            }
        });
        
        JavaPairRDD<String, ArrayList<Tuple2<Long, String>>> prePlisAsList = 
            prePlis.mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, String>>,
                String, ArrayList<Tuple2<Long, String>>>(){
                    public Tuple2<String, ArrayList<Tuple2<Long, String>>> 
                        call(Tuple2<String, Tuple2<Long, String>> tuple){
                            ArrayList<Tuple2<Long, String>> temp = new ArrayList<>();
                            temp.add(tuple._2);
                            return new Tuple2<String, ArrayList<Tuple2<Long, String>>>(tuple._1,
                                temp);
                        }
                    }
        );
        
        System.out.println(prePlisAsList.take(100));
    }

    

    /*public List<PositionListIndex> getPLIList() throws InputIterationException {

        List<PositionListIndex> result = new ArrayList<>();
        //TODO continue method
        JavaRDD<List<PositionListIndex>> rawPlis = getRawPlis();
        return result;
    }*/

}
