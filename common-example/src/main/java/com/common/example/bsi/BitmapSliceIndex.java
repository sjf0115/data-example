package com.common.example.bsi;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public interface BitmapSliceIndex {
    enum Operation {
        // EQ equal
        EQ,
        // NEQ not equal
        NEQ,
        // LE less than or equal
        LE,
        // LT less than
        LT,
        // GE greater than or equal
        GE,
        // GT greater than
        GT,
        // RANGE range
        RANGE
    }

    int bitCount();
    long getLongCardinality();
    void addValue(Pair<Integer, Integer> pair);
    void addValue(int key, int value);
    Pair<Integer, Boolean> getValue(int key);
    void setValues(List<Pair<Integer, Integer>> values, Integer currentMaxValue, Integer currentMinValue);
    boolean valueExist(int key);

    void serialize(ByteBuffer buffer) throws IOException;
    void serialize(DataOutput output) throws IOException;
    int serializedSizeInBytes();

    void deserialize(ByteBuffer buffer) throws IOException;
    void deserialize(DataInput in) throws IOException;

}
