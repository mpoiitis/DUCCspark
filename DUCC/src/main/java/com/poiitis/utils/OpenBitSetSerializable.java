
package com.poiitis.utils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
        super();
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
    
    public void writeObject(ObjectOutputStream out) throws IOException{
        out.defaultWriteObject();
        out.writeObject(this.getBits());
        out.writeInt(this.getNumWords());
    }
    
    public void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{
        System.out.println("============================================ " + this.getClass());
        in.defaultReadObject();
        this.setBits((long[]) in.readObject());
        this.setNumWords(in.readInt());
    }
}
