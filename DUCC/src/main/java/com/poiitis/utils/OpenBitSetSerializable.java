/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.poiitis.utils;

import java.io.Serializable;
import org.apache.lucene.util.OpenBitSet;

/**
 *
 * @author Poiitis Marinos
 */
public class OpenBitSetSerializable extends OpenBitSet implements Serializable{

    private static final long serialVersionUID = -7373615923480249309L;
    
    
    public OpenBitSetSerializable(){
        super();
    }
    
    public OpenBitSetSerializable(long numBits){
        super(numBits);
    }
    
    public OpenBitSetSerializable(long[] bits, int numWords){
        super(bits, numWords);
    }
 
    public OpenBitSetSerializable(OpenBitSet obs) {
        this.setBitSet(obs);
    }
    
    public OpenBitSet getAsOpenBitSet() {

        OpenBitSet copy=new OpenBitSet();
        copy.setBits(this.getBits());
        copy.setNumWords(this.getNumWords());
        return copy;
    }    
    
    public void setBitSet(OpenBitSet obs){
        this.setBits(obs.getBits());
        this.setNumWords(obs.getNumWords());
    }
    
}
