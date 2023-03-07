package com.common.example.bsi;

import org.apache.hadoop.io.WritableUtils;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * 功能：RoaringBitmapSliceIndex
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/3/7 下午11:44
 */
public class RoaringBitmapSliceIndex implements BitmapSliceIndex {
    private int maxValue;
    private int minValue;
    private RoaringBitmap[] bA;
    private RoaringBitmap ebM;
    private Boolean runOptimized = false;

    public RoaringBitmapSliceIndex(int minValue, int maxValue) {
        // 最大值二进制个数
        int size = 32 - Integer.numberOfLeadingZeros(maxValue);
        bA = new RoaringBitmap[size];
        for (int i = 0; i < size; i++) {
            bA[i] = new RoaringBitmap();
        }
        this.ebM = new RoaringBitmap();
        this.maxValue = maxValue;
        this.minValue = minValue;
    }

    // 最大值和最小值可选 如果没有指定自动分配
    public RoaringBitmapSliceIndex() {
        this(0, 0);
    }


    // 切片数组个数
    @Override
    public int bitCount() {
        return this.bA.length;
    }

    @Override
    public long getLongCardinality() {
        return this.ebM.getLongCardinality();
    }

    @Override
    public void setValue(int columnId, int value) {
        ensureCapacityInternal(0, value);
        for (int i = 0; i < this.bitCount(); i++) {
            if ((value & (1 << i)) > 0) {
                this.bA[i].add(columnId);
            } else {
                this.bA[i].remove(columnId);
            }
        }
        this.ebM.add(columnId);
    }

    @Override
    public Pair<Integer, Boolean> getValue(int columnId) {
        boolean exists = this.ebM.contains(columnId);
        if (!exists) {
            return Pair.newPair(0, false);
        }
        int value = 0;
        for (int i = 0; i < this.bitCount(); i++) {
            if (this.bA[i].contains(columnId)) {
                value |= (1 << i);
            }
        }
        return Pair.newPair(value, true);
    }

    @Override
    public void setValues(List<Pair<Integer, Integer>> values, Integer currentMaxValue, Integer currentMinValue) {

    }

    @Override
    public void serialize(ByteBuffer buffer) throws IOException {
        // write meta
        buffer.putInt(this.minValue);
        buffer.putInt(this.maxValue);
        buffer.put(this.runOptimized ? (byte) 1 : (byte) 0);
        // write ebm
        this.ebM.serialize(buffer);

        // write ba
        buffer.putInt(this.bA.length);
        for (RoaringBitmap rb : this.bA) {
            rb.serialize(buffer);
        }
    }

    @Override
    public void serialize(DataOutput output) throws IOException {
        // write meta
        WritableUtils.writeVInt(output, minValue);
        WritableUtils.writeVInt(output, maxValue);
        output.writeBoolean(this.runOptimized);

        // write ebm
        this.ebM.serialize(output);

        // write ba
        WritableUtils.writeVInt(output, this.bA.length);
        for (RoaringBitmap rb : this.bA) {
            rb.serialize(output);
        }
    }

    @Override
    public int serializedSizeInBytes() {
        return 0;
    }

    // 自动扩容
    private void ensureCapacityInternal(int minValue, int maxValue) {
        // 如果最大值和最小值设置为 0 自动分配切片数组的大小
        if (this.maxValue == 0 && this.minValue == 0) {
            this.maxValue = maxValue;
            this.minValue = minValue;
            this.bA = new RoaringBitmap[Integer.toBinaryString(maxValue).length()];
            for (int i = 0; i < this.bA.length; i++) {
                this.bA[i] = new RoaringBitmap();
            }
        } else if (maxValue > this.maxValue) {
            int newBitDepth = Integer.toBinaryString(maxValue).length();
            int oldBitDepth = this.bA.length;
            grow(newBitDepth, oldBitDepth);
            this.maxValue = maxValue;
        }
    }

    // 扩容
    private void grow(int newBitDepth, int oldBitDepth) {
        RoaringBitmap[] newBA = new RoaringBitmap[newBitDepth];
        System.arraycopy(this.bA, 0, newBA, 0, oldBitDepth);
        for (int i = newBitDepth - 1; i >= oldBitDepth; i--) {
            newBA[i] = new RoaringBitmap();
            if (this.runOptimized) {
                newBA[i].runOptimize();
            }
        }
        this.bA = newBA;
    }
}
