package com.poiitis.pli;

import com.google.common.collect.ArrayListMultimap;
import com.poiitis.ducc.Adult;
import com.poiitis.exceptions.InputIterationException;;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
    protected JavaPairRDD<String, ArrayListMultimap<String, Long>> plis;

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
        JavaPairRDD<String, Tuple2<Long,String>> prePlis = adults.flatMapToPair((Adult adult) -> {
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
        });
        
        //transform tuple to arraylist<tuple> so as to reduce them in plis
        JavaPairRDD<String, ArrayList<Tuple2<Long, String>>> prePlisAsList = 
            prePlis.mapToPair((Tuple2<String, Tuple2<Long, String>> tuple) -> {
                ArrayList<Tuple2<Long, String>> temp = new ArrayList<>();
                temp.add(tuple._2);
                return new Tuple2<>(tuple._1, temp);
        });
        
        //group by column
        JavaPairRDD<String, ArrayList<Tuple2<Long, String>>> groupsByColumn = 
            prePlisAsList.reduceByKey((ArrayList<Tuple2<Long, String>> x, 
                ArrayList<Tuple2<Long, String>> y) -> {
                x.addAll(y);
                return x;
        });
        
        //extract final PLIs  
        JavaPairRDD<String, ArrayListMultimap<String, Long>> tempPlis = 
                groupsByColumn.mapToPair((Tuple2<String,
                ArrayList<Tuple2<Long, String>>> tuple) -> {
            ArrayListMultimap<String, Long> multimap = ArrayListMultimap.create();
            
            for(Tuple2<Long, String> t : tuple._2){
                multimap.put(t._2, t._1);//cell value as key, rowNum as value
            }
            
            return new Tuple2<>(tuple._1, multimap);
        });
        
        plis = purgePLIEntries(tempPlis);
       
    }
    
    /**
     * 
     *Removes PLIs that contain cellValues which correspond to a unique row 
     * for a specific column
     */
    public JavaPairRDD<String, ArrayListMultimap<String, Long>> purgePLIEntries(JavaPairRDD<String,
            ArrayListMultimap<String, Long>> rdd){        
        
        JavaPairRDD<String, ArrayListMultimap<String, Long>> purgedPlis = 
            rdd.mapToPair((Tuple2<String, ArrayListMultimap<String, Long>> tuple) -> {
                Iterator<String> keyIterator = tuple._2.keySet().iterator();
                while(keyIterator.hasNext()){
                    String key = keyIterator.next();
                    Collection<Long> values = tuple._2.get(key);
                    if(values.size() < 2){
                        keyIterator.remove();
                    }
                }
                
                return new Tuple2<>(tuple._1, tuple._2);
        });
        
        return purgedPlis;
    }

}
