package th.hadoop.play.hac;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Random;
import org.apache.hadoop.io.Writable;

public class BloomFilter<E> implements Writable {

    private int k = 7;
    private int size = 9999999;
    private BitSet bits;

    public BloomFilter() {
        bits = new BitSet(size);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int size_in_bytes = size/8;
        byte[] bytes = new byte[size_in_bytes];
                
        for (int i=0; i < size_in_bytes; i++) {
            byte nextByte = 0;
            for (int j =0; j < 8; j++) {
                if (bits.get(8*i + j)) {
                    nextByte |= 1 << j; 
                }
            }
            bytes[i] = nextByte;
        }
        
        out.write(bytes);
        
    }

    

    @Override
    public void readFields(DataInput in) throws IOException {
        int size_in_bytes = size/8;
        byte[] bytes = new byte[size_in_bytes];
        in.readFully(bytes);
        
        for (int i = 0; i < size_in_bytes; i++) {
            byte nextByte = bytes[i];
            for (int j =0; j < 8; j++) {
                if ( ( (int)nextByte & (1<<j )) != 0) {
                    bits.set(8* i + j);
                }
            }            
        }        
    }
    
    protected int[] getIndexes(E entry) {

        long seed = 0;

        // md5 hash of the entry
        byte[] hash;

        int[] indices = new int[k];
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(entry.toString().getBytes());
            hash = md5.digest();
            for (int i = 0; i < k; i++) {
                seed = seed ^ (((long)hash[i] & 0xFF)) <<8*i;
            }            
        } catch (NoSuchAlgorithmException ex) {
        }

        Random gen = new Random(seed);
        for (int i = 0; i <k; i++ ) {
            indices[i] = gen.nextInt(size);
        }
        return indices;
    }

    public void put(E entry) {
        int[] indices = getIndexes(entry);
        for (int i : indices) {
            bits.set(i);
        }
    }

    public boolean contains(E entry) {
        int[] indices = getIndexes(entry);
        for (int i : indices) {
            if (!bits.get(i)) {
                return false;
            }
        }
        return true;
    }

    public void union(BloomFilter<E> filter2) {
        bits.or(filter2.bits);
    }
    
}
