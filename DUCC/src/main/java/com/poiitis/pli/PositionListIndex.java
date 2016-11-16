package com.poiitis.pli;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import scala.Tuple2;

/**
 *
 * @author Poiitis Marinos Position list indices (or stripped partitions) are an
 * index structure that stores the positions of equal values in a nested list. A
 * column with the values a, a, b, c, b, c transfers to the position list index
 * ((0, 1), (2, 4), (3, 5)). Clusters of size 1 are discarded.
 */
public class PositionListIndex implements Serializable{

    private static final long serialVersionUID = 5832532149217053117L;

    protected ArrayList<Tuple2<String,Integer>> name;//may contain multiple columns
    protected List<IntArrayList> clusters;
    protected long rawKeyError = -1;

    public PositionListIndex(List<IntArrayList> clusters) {
        this.clusters = clusters;
    }
    
    public PositionListIndex(Tuple2<String,Integer> name, List<IntArrayList> clusters) {
        if(this.name == null){
            this.name = new ArrayList<>();
        }
        
        this.name.add(name);
        this.clusters = clusters;
    }
    
    public PositionListIndex(ArrayList<Tuple2<String,Integer>> names, List<IntArrayList> clusters) {
        if(this.name == null){
            this.name = new ArrayList<>();
        }
        
        this.name = names;
        this.clusters = clusters;
    }

    /**
     * Constructs an empty {@link PositionListIndex}.
     */
    public PositionListIndex() {
        this.clusters = new ArrayList<>();
    }

    /**
     * Intersects the given PositionListIndex with this PositionListIndex
     * returning a new PositionListIndex. For the intersection the smaller
     * PositionListIndex is converted into a HashMap.
     *
     * @param otherPLI the other {@link PositionListIndex} to intersect
     * @return the intersected {@link PositionListIndex}
     */
    public PositionListIndex intersect(PositionListIndex otherPLI) {
        //TODO Optimize Smaller PLI as Hashmap?
        return calculateIntersection(otherPLI);
    }

    public ArrayList<Tuple2<String,Integer>> getName(){
        return name;
    }
    
    public List<IntArrayList> getClusters() {
        return clusters;
    }

    /**
     * Creates a complete (deep) copy 
     *
     * @return cloned PositionListIndex
     */
    @Override
    public PositionListIndex clone() {
        List<IntArrayList> newClusters = new ArrayList<>();
        for (IntArrayList cluster : clusters) {
            newClusters.add(cluster.clone());
        }
        
        PositionListIndex clone = new PositionListIndex(newClusters);
        clone.name = this.name;
        clone.rawKeyError = this.rawKeyError;
        return clone;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        List<IntOpenHashSet> setCluster = convertClustersToSets(clusters);

        Collections.sort(setCluster, new Comparator<IntSet>() {

            @Override
            public int compare(IntSet o1, IntSet o2) {
                return o1.hashCode() - o2.hashCode();
            }
        });
        result = prime * result + (setCluster.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PositionListIndex other = (PositionListIndex) obj;
        if (clusters == null) {
            if (other.clusters != null) {
                return false;
            }
        } else {
            List<IntOpenHashSet> setCluster = convertClustersToSets(clusters);
            List<IntOpenHashSet> otherSetCluster = convertClustersToSets(other.clusters);

            for (IntOpenHashSet cluster : setCluster) {
                if (!otherSetCluster.contains(cluster)) {
                    return false;
                }
            }
            for (IntOpenHashSet cluster : otherSetCluster) {
                if (!setCluster.contains(cluster)) {
                    return false;
                }
            }
        }

        return true;
    }

    protected List<IntOpenHashSet> convertClustersToSets(List<IntArrayList> listCluster) {
        List<IntOpenHashSet> setClusters = new LinkedList<>();
        for (IntList cluster : listCluster) {
            setClusters.add(new IntOpenHashSet(cluster));
        }

        return setClusters;
    }

    /**
     * Intersects the two given {@link PositionListIndex} and returns the
     * outcome as new PositionListIndex.
     *
     * @param otherPLI the other {@link PositionListIndex} to intersect
     * @return the intersected {@link PositionListIndex}
     */
    protected PositionListIndex calculateIntersection(PositionListIndex otherPLI) {
        Int2IntOpenHashMap hashedPLI = this.asHashMap();
        Map<IntPair, IntArrayList> map = new HashMap<>();
        buildMap(otherPLI, hashedPLI, map);

        List<IntArrayList> clusters = new ArrayList<>();
        for (IntArrayList cluster : map.values()) {
            if (cluster.size() < 2) {
                continue;
            }
            clusters.add(cluster);
        }
        
        ArrayList<Tuple2<String,Integer>> newName = new ArrayList<>();
        for(Tuple2<String,Integer> tuple : this.getName()){
            newName.add(tuple);
        }
        for(Tuple2<String,Integer> tuple : otherPLI.getName()){
            newName.add(tuple);
        }
        return new PositionListIndex(newName,clusters);
    }

    protected void buildMap(PositionListIndex otherPLI, Int2IntOpenHashMap hashedPLI,
            Map<IntPair, IntArrayList> map) {
        int uniqueValueCount = 0;
        for (IntArrayList sameValues : otherPLI.clusters) {
            for (int rowCount : sameValues) {
                if (hashedPLI.containsKey(rowCount)) {
                    IntPair pair = new IntPair(uniqueValueCount, hashedPLI.get(rowCount));
                    updateMap(map, rowCount, pair);
                }
            }
            uniqueValueCount++;
        }
    }

    protected void updateMap(Map<IntPair, IntArrayList> map, int rowCount, IntPair pair) {
        if (map.containsKey(pair)) {
            IntArrayList currentList = map.get(pair);
            currentList.add(rowCount);
        } else {
            IntArrayList newList = new IntArrayList();
            newList.add(rowCount);
            map.put(pair, newList);
        }
    }

    /**
     * Returns the position list index in a map representation. Every row index
     * maps to a value reconstruction. As the original values are unknown they
     * are represented by a counter. The position list index ((0, 1), (2, 4),
     * (3, 5)) would be represented by {0=0, 1=0, 2=1, 3=2, 4=1, 5=2}.
     *
     * @return the pli as hash map
     */
    public Int2IntOpenHashMap asHashMap() {
        Int2IntOpenHashMap hashedPLI = new Int2IntOpenHashMap(clusters.size());
        int uniqueValueCount = 0;
        for (IntArrayList sameValues : clusters) {
            for (int rowIndex : sameValues) {
                hashedPLI.put(rowIndex, uniqueValueCount);
            }
            uniqueValueCount++;
        }
        return hashedPLI;
    }

    /**
     * Returns the number of non unary clusters.
     *
     * @return the number of clusters in the {@link PositionListIndex}
     */
    public long size() {
        return clusters.size();
    }

    /**
     * @return the {@link PositionListIndex} contains only unary clusters.
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * @return the column represented by the {@link PositionListIndex} is
     * unique.
     */
    public boolean isUnique() {
        return isEmpty();
    }

    /**
     * Returns the number of columns to remove in order to make column unique.
     * (raw key error)
     *
     * @return raw key error
     */
    public long getRawKeyError() {
        if (rawKeyError == -1) {
            rawKeyError = calculateRawKeyError();
        }

        return rawKeyError;
    }

    protected long calculateRawKeyError() {
        long sumClusterSize = 0;

        for (IntArrayList cluster : clusters) {
            sumClusterSize += cluster.size();
        }

        return sumClusterSize - clusters.size();
    }

}
