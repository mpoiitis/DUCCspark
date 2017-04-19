package com.poiitis.ducc;

import com.google.common.collect.ImmutableList;
import com.poiitis.utils.Singleton;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *
 * @author Poiitis Marinos
 */
public class CustomParser implements Serializable{

    private static final long serialVersionUID = -3808214153694185619L;
    
    private JavaRDD<String> input;
    private ArrayList<Tuple2<String,Integer>> columnNames;
    
    public CustomParser(JavaRDD<String> input){    
        this.input = input;
        columnNames = new ArrayList<>();
        
        columnNames.add(new Tuple2("Age", 0));
        columnNames.add(new Tuple2("Name", 1));
        columnNames.add(new Tuple2("Job", 2));
        columnNames.add(new Tuple2("Country", 3));
    }
    
    public ArrayList<Tuple2<String,Integer>> getColumnNames(){return columnNames;}
    
    public JavaRDD<Adult> parseFile(){
        
        JavaPairRDD<String, Long> temp = this.input.zipWithIndex();

        Broadcast<ArrayList<Tuple2<String,Integer>>> bColNames = Singleton.getSparkContext().broadcast(this.columnNames);
        
        JavaRDD<Adult> rdd_adults = temp.map((Tuple2<String, Long> tuple) -> {
            String[] fields = tuple._1.split(",");
            //turn array to list
            List<String> temp1 = ImmutableList.copyOf(fields);
            ArrayList<String> fieldsList = new ArrayList<>(temp1);
            Adult adult = new Adult(bColNames.value(), fieldsList,
                    tuple._2.intValue());
            return adult;
        });
        return rdd_adults;
    }
}
