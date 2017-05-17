package com.poiitis.pli;

import com.poiitis.ducc.Adult;
import com.poiitis.exceptions.InputIterationException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

/**
 *
 * @author Poiitis Marinos
 */
public class PLIBuilder implements Serializable{

    private static final long serialVersionUID = 4997530231064808611L;

    protected int numberOfTuples = -1;
    protected boolean nullEqualsNull;
    protected JavaRDD<Adult> adults;
    protected JavaPairRDD<Tuple2<String,Integer>, HashMap<String, ArrayList<Integer>>> plis;

    public PLIBuilder(JavaRDD<Adult> adults) {
        this.adults = adults;
        this.adults = this.adults.cache();
        this.nullEqualsNull = true;
    }

    public PLIBuilder(JavaRDD<Adult> adults, boolean nullEqualsNull) {
        this.adults = adults;
        this.adults = this.adults.cache();
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
    public int getNumberOfTuples() throws InputIterationException {
        if (this.numberOfTuples == -1) {
            throw new InputIterationException();
        } else {
            return this.numberOfTuples;
        }
    }

    public void createInitialPLIs() {

        this.adults = this.adults.cache();
        this.numberOfTuples = (int) this.adults.count();
        
        //list of all items of all adults in form (colName,colIndex),(row,cellValue)
        JavaPairRDD<Tuple2<String,Integer>, Tuple2<Integer,String>> prePlis = this.adults.flatMapToPair((Adult adult) -> {
            ArrayList<Tuple2<Tuple2<String,Integer>, Tuple2<Integer, String>>> result = new
                                ArrayList<>();
            LinkedHashMap<Tuple2<String,Integer>, String> lhmap = adult.getAttributes();
            for(Map.Entry<Tuple2<String,Integer>,String> entry : lhmap.entrySet()){
                Tuple2<Integer, String> innerTuple = new Tuple2<>(adult.getLineNumber(),
                        entry.getValue());
                Tuple2<Tuple2<String,Integer>, Tuple2<Integer, String>> tuple =
                        new Tuple2<>(entry.getKey(), innerTuple);
                
                result.add(tuple);
            }
            return result.iterator();
        });

        //transform tuple to arraylist<tuple> so as to reduce them in plis
        JavaPairRDD<Tuple2<String,Integer>, ArrayList<Tuple2<Integer, String>>> prePlisAsList = 
            prePlis.mapToPair((Tuple2<Tuple2<String, Integer>, Tuple2<Integer, String>> tuple) -> {
                ArrayList<Tuple2<Integer, String>> temp = new ArrayList<>();
                temp.add(tuple._2);
                return new Tuple2<>(tuple._1, temp);
        });
        
        //group by column
        JavaPairRDD<Tuple2<String,Integer>, ArrayList<Tuple2<Integer, String>>> groupsByColumn = 
            prePlisAsList.reduceByKey((ArrayList<Tuple2<Integer, String>> x, 
                ArrayList<Tuple2<Integer, String>> y) -> {
                x.addAll(y);
                return x;
        });
        
        //extract final PLIs  
        JavaPairRDD<Tuple2<String,Integer>, HashMap<String, ArrayList<Integer>>> tempPlis = 
                groupsByColumn.mapToPair((Tuple2<Tuple2<String,Integer>,
                ArrayList<Tuple2<Integer, String>>> tuple) -> {
            HashMap<String, ArrayList<Integer>> multimap = new HashMap<>();
            for(Tuple2<Integer, String> t : tuple._2){
                ArrayList<Integer> get = multimap.get(t._2);
                if( get != null){
                    get.add(t._1);
                }
                else
                {
                    ArrayList<Integer> temp = new ArrayList<>();
                    temp.add(t._1);
                    multimap.put(t._2, temp);
                }
            }

            return new Tuple2<>(tuple._1, multimap);
        });

        this.plis = purgePLIEntries(tempPlis);      
    }
    
    /**
     * 
     *Removes PLIs that contain cellValues which correspond to a unique row 
     * for a specific column
     */
    public JavaPairRDD<Tuple2<String,Integer>, HashMap<String, ArrayList<Integer>>> purgePLIEntries(JavaPairRDD<Tuple2<String,Integer>,
            HashMap<String, ArrayList<Integer>>> rdd){        
        
        JavaPairRDD<Tuple2<String,Integer>, HashMap<String, ArrayList<Integer>>> purgedPlis = 
            rdd.mapToPair((Tuple2<Tuple2<String,Integer>, HashMap<String, ArrayList<Integer>>> tuple) -> {
                Iterator<String> keyIterator = tuple._2.keySet().iterator();
                while(keyIterator.hasNext()){
                    String key = keyIterator.next();
                    Collection<Integer> values = tuple._2.get(key);
                    if(values.size() < 2){
                        keyIterator.remove();
                    }
                }
                
                return new Tuple2<>(tuple._1, tuple._2);
        });

        return purgedPlis;
    }
    
    /**
     * Creates PositionListIndex items from the given multimap
     * and returns them as a list
     */
    public JavaRDD<PositionListIndex> getPLIList(){
        JavaRDD<PositionListIndex> result = plis.map((Tuple2<Tuple2<String,Integer>, 
                HashMap<String, ArrayList<Integer>>> tuple) -> {
            List<IntArrayList> list = new ArrayList<>();
            for(String key : tuple._2.keySet()){
                IntArrayList intList = new IntArrayList(tuple._2.get(key));
                list.add(intList);
            }
            
            return new PositionListIndex(tuple._1, list);
        });
        this.adults.unpersist();// it is the last called function
        return result;
    }

}
