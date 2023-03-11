package com.common.example.bsi;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.io.WritableUtils;
import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * 功能：RoaringBitmapSliceIndex
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2023/3/7 下午11:44
 */
public class RoaringBitmapSliceIndex implements BitmapSliceIndex {
    // 存储值的最大值
    private int maxValue;
    // 存储值的最小值
    private int minValue;
    // 切片 Bitmap 数组
    private RoaringBitmap[] bitmaps;
    // Not NULL Bitmap 表示全量
    private RoaringBitmap ebM;
    private Boolean runOptimized = false;

    public RoaringBitmapSliceIndex(int minValue, int maxValue) {
        // 根据最大值二进制基数(位数)计算切片个数
        int size = 32 - Integer.numberOfLeadingZeros(maxValue);
        bitmaps = new RoaringBitmap[size];
        for (int i = 0; i < size; i++) {
            bitmaps[i] = new RoaringBitmap();
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
        return this.bitmaps.length;
    }

    // 全量基数
    @Override
    public long getLongCardinality() {
        return this.ebM.getLongCardinality();
    }

    // 添加元素
    @Override
    public void addValue(Pair<Integer, Integer> pair) {
        addValue(pair.getKey(), pair.getValue());
    }

    // 添加元素
    @Override
    public void addValue(int key, int value) {
        ensureCapacityInternal(0, value);
        for (int i = 0; i < this.bitCount(); i++) {
            if ((value & (1 << i)) > 0) {
                //
                this.bitmaps[i].add(key);
            } else {
                this.bitmaps[i].remove(key);
            }
        }
        this.ebM.add(key);
    }

    // 获取对应值
    @Override
    public Pair<Integer, Boolean> getValue(int key) {
        boolean exists = this.ebM.contains(key);
        if (!exists) {
            return Pair.newPair(0, false);
        }
        int value = 0;
        for (int i = 0; i < this.bitCount(); i++) {
            if (this.bitmaps[i].contains(key)) {
                value |= (1 << i);
            }
        }
        return Pair.newPair(value, true);
    }

    // 批量添加
    @Override
    public void setValues(List<Pair<Integer, Integer>> values, Integer currentMaxValue, Integer currentMinValue) {
        int maxValue = currentMaxValue != null ? currentMaxValue : values.stream().mapToInt(Pair::getRight).max().getAsInt();
        int minValue = currentMinValue != null ? currentMinValue : values.stream().mapToInt(Pair::getRight).min().getAsInt();
        ensureCapacityInternal(minValue, maxValue);
        for (Pair<Integer, Integer> pair : values) {
            this.addValue(pair.getKey(), pair.getValue());
        }
    }

    @Override
    public boolean valueExist(int key) {
        return this.ebM.contains(key);
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
        buffer.putInt(this.bitmaps.length);
        for (RoaringBitmap rb : this.bitmaps) {
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
        WritableUtils.writeVInt(output, this.bitmaps.length);
        for (RoaringBitmap rb : this.bitmaps) {
            rb.serialize(output);
        }
    }

    @Override
    public int serializedSizeInBytes() {
        int size = 0;
        for (RoaringBitmap rb : this.bitmaps) {
            size += rb.serializedSizeInBytes();
        }
        return 4 + 4 + 1 + 4 + this.ebM.serializedSizeInBytes() + size;
    }

    @Override
    public void deserialize(ByteBuffer buffer) throws IOException {
        this.clear();
        // read meta
        this.minValue = buffer.getInt();
        this.maxValue = buffer.getInt();
        this.runOptimized = buffer.get() == (byte) 1;

        // read ebm
        RoaringBitmap ebm = new RoaringBitmap();
        ebm.deserialize(buffer);
        this.ebM = ebm;
        // read ba
        buffer.position(buffer.position() + ebm.serializedSizeInBytes());
        int bitDepth = buffer.getInt();
        RoaringBitmap[] ba = new RoaringBitmap[bitDepth];
        for (int i = 0; i < bitDepth; i++) {
            RoaringBitmap rb = new RoaringBitmap();
            rb.deserialize(buffer);
            ba[i] = rb;
            buffer.position(buffer.position() + rb.serializedSizeInBytes());
        }
        this.bitmaps = ba;
    }

    @Override
    public void deserialize(DataInput in) throws IOException {
        this.clear();

        // read meta
        this.minValue = WritableUtils.readVInt(in);
        this.maxValue = WritableUtils.readVInt(in);
        this.runOptimized = in.readBoolean();

        // read ebm
        RoaringBitmap ebm = new RoaringBitmap();
        ebm.deserialize(in);
        this.ebM = ebm;

        // read ba
        int bitDepth = WritableUtils.readVInt(in);
        RoaringBitmap[] ba = new RoaringBitmap[bitDepth];
        for (int i = 0; i < bitDepth; i++) {
            RoaringBitmap rb = new RoaringBitmap();
            rb.deserialize(in);
            ba[i] = rb;
        }
        this.bitmaps = ba;
    }

    //------------------------------------------------------------------------------------------------------------------

    // 自动扩容
    private void ensureCapacityInternal(int minValue, int maxValue) {
        // 如果最大值和最小值设置为 0 自动分配切片数组的大小
        if (this.maxValue == 0 && this.minValue == 0) {
            this.maxValue = maxValue;
            this.minValue = minValue;
            this.bitmaps = new RoaringBitmap[Integer.toBinaryString(maxValue).length()];
            for (int i = 0; i < this.bitmaps.length; i++) {
                this.bitmaps[i] = new RoaringBitmap();
            }
        } else if (maxValue > this.maxValue) {
            int newBitDepth = Integer.toBinaryString(maxValue).length();
            int oldBitDepth = this.bitmaps.length;
            grow(newBitDepth, oldBitDepth);
            this.maxValue = maxValue;
        }
    }

    // 扩容
    private void grow(int newBitDepth, int oldBitDepth) {
        RoaringBitmap[] newBA = new RoaringBitmap[newBitDepth];
        System.arraycopy(this.bitmaps, 0, newBA, 0, oldBitDepth);
        for (int i = newBitDepth - 1; i >= oldBitDepth; i--) {
            newBA[i] = new RoaringBitmap();
            if (this.runOptimized) {
                newBA[i].runOptimize();
            }
        }
        this.bitmaps = newBA;
    }

    // 清空
    private void clear() {
        this.maxValue = 0;
        this.minValue = 0;
        this.ebM = null;
        this.bitmaps = null;
    }

    //------------------------------------------------------------------------------------------------------------------

    // 范围查询
    private RoaringBitmap compare(BitmapSliceIndex.Operation operation, int predicate, RoaringBitmap foundSet) {
        RoaringBitmap fixedFoundSet = foundSet == null ? this.ebM : foundSet;
        // 划分成三部分：小于(初始为空)、等于(初始为全量)、大于(初始为空)
        RoaringBitmap GT = new RoaringBitmap();
        RoaringBitmap LT = new RoaringBitmap();
        RoaringBitmap EQ = this.ebM;

        for (int i = this.bitCount() - 1; i >= 0; i--) {
            // 右移 i 位 查看查找值 i+1 Bit 位
            int bit = (predicate >> i) & 1;
            if (bit == 1) {
                // 查找值 Bit 位为 1 则 EQ 中的候选值小于等于查找值
                // 即 EQ 中的候选值分为两部分：一部分继续留在 EQ 集合中；一部分转移到 LT 集合中

                // EQ 集合中的候选值 Bit 位为 0 (EQ 集合与 bitmaps[i] 集合的差集) 则转移到 LT 集合中(与 LT 集合的并集)
                LT = RoaringBitmap.or(LT, RoaringBitmap.andNot(EQ, this.bitmaps[i]));
                // EQ 集合中的候选值 Bit 位为 1 (EQ 集合与 bitmaps[i] 集合的交集) 则继续留在 EQ 集合中
                EQ = RoaringBitmap.and(EQ, this.bitmaps[i]);
            } else {
                // 查找值 Bit 位为 0 则 EQ 中的候选值大于等于查找值
                // 即 EQ 中的候选值分为两部分：一部分继续留在 EQ 集合中；一部分转移到 GT 集合中

                // EQ 集合中的候选值 Bit 位为 0 (EQ 集合与 bitmaps[i] 集合的交集) 则转移到 GT 集合中(与 GT 集合的并集)
                GT = RoaringBitmap.or(GT, RoaringBitmap.and(EQ, this.bitmaps[i]));
                // EQ 集合中的候选值 Bit 位为 1 (EQ 集合与 bitmaps[i] 集合的差集) 则继续留在 EQ 集合中
                EQ = RoaringBitmap.andNot(EQ, this.bitmaps[i]);
            }
        }

        EQ = RoaringBitmap.and(fixedFoundSet, EQ);
        switch (operation) {
            case EQ:
                return EQ;
            case NEQ:
                return RoaringBitmap.andNot(fixedFoundSet, EQ);
            case GT:
                return RoaringBitmap.and(GT, fixedFoundSet);
            case LT:
                return RoaringBitmap.and(LT, fixedFoundSet);
            case LE:
                return RoaringBitmap.or(LT, EQ);
            case GE:
                return RoaringBitmap.or(GT, EQ);
            default:
                throw new IllegalArgumentException("");
        }
    }

    // 优化版本
    private RoaringBitmap compare2(BitmapSliceIndex.Operation operation, int vale) {
        // 大于查找值 Value 的集合
        RoaringBitmap GT = new RoaringBitmap();
        // 小于查找值 Value 的集合
        RoaringBitmap LT = new RoaringBitmap();
        // 等于查找值 Value 的集合
        RoaringBitmap EQ = new RoaringBitmap();
        // 候选集合 初始化为全量
        RoaringBitmap CANDIDATE = this.ebM;

        for (int i = this.bitCount() - 1; i >= 0; i--) {
            // 右移 i 位 查看查找值 i+1 Bit 位
            int bit = (vale >> i) & 1;
            if (bit == 1) {
                // 查找值 Bit 位为 1 则候选集合中的元素值均小于等于查找值
                // 将候选集合中小于查找值的元素(即 Bit 位为 0，候选集合与 bitmaps[i] 集合的差集)转移到 LT 集合中
                LT = RoaringBitmap.or(LT, RoaringBitmap.andNot(CANDIDATE, this.bitmaps[i]));
            } else {
                // 查找值 Bit 位为 0 则候选集合中的元素值均大于等于查找值
                // 将候选集合中大于查找值的元素(即 Bit 位为 1，候选集合与 bitmaps[i] 集合的交集)转移到 GT 集合中
                GT = RoaringBitmap.or(GT, RoaringBitmap.and(CANDIDATE, this.bitmaps[i]));
            }
        }
        // 将候选集合中大于或者等于查找值的值分别转移到 GT 和 LT 集合中 剩下的是等于查找值的 Key
        EQ = RoaringBitmap.or(CANDIDATE, EQ);

        switch (operation) {
            case EQ:
                return EQ;
            case NEQ:
                return RoaringBitmap.andNot(this.ebM, EQ);
            case GT:
                return GT;
            case LT:
                return LT;
            case LE:
                return RoaringBitmap.or(LT, EQ);
            case GE:
                return RoaringBitmap.or(GT, EQ);
            default:
                throw new IllegalArgumentException("");
        }
    }

    public RoaringBitmap eq(int predicate) {
        return compare(Operation.EQ, predicate, null);
    }

    public RoaringBitmap neq(int predicate) {
        return compare(Operation.NEQ, predicate, null);
    }

    public RoaringBitmap gt(int predicate) {
        return compare(Operation.GT, predicate, null);
    }

    public RoaringBitmap lt(int predicate) {
        return compare(Operation.LT, predicate, null);
    }

    public RoaringBitmap gte(int predicate) {
        return compare(Operation.GE, predicate, null);
    }

    public RoaringBitmap lte(int predicate) {
        return compare(Operation.LE, predicate, null);
    }

    public RoaringBitmap range(int start, int end) {
        RoaringBitmap left = compare(Operation.GE, start, null);
        RoaringBitmap right = compare(Operation.LE, end, null);
        return RoaringBitmap.and(left, right);
    }

    public RoaringBitmap eq(int predicate, RoaringBitmap foundSet) {
        return compare(Operation.EQ, predicate, foundSet);
    }

    public RoaringBitmap neq(int predicate, RoaringBitmap foundSet) {
        return compare(Operation.NEQ, predicate, foundSet);
    }

    public RoaringBitmap gt(int predicate, RoaringBitmap foundSet) {
        return compare(Operation.GT, predicate, foundSet);
    }

    public RoaringBitmap lt(int predicate, RoaringBitmap foundSet) {
        return compare(Operation.LT, predicate, foundSet);
    }

    public RoaringBitmap gte(int predicate, RoaringBitmap foundSet) {
        return compare(Operation.GE, predicate, foundSet);
    }

    public RoaringBitmap lte(int predicate, RoaringBitmap foundSet) {
        return compare(Operation.LE, predicate, foundSet);
    }

    public RoaringBitmap range(int start, int end, RoaringBitmap foundSet) {
        RoaringBitmap left = compare(Operation.GE, start, foundSet);
        RoaringBitmap right = compare(BitmapSliceIndex.Operation.LE, end, foundSet);
        return RoaringBitmap.and(left, right);
    }

    //------------------------------------------------------------------------------------------------------------------

    public void add(RoaringBitmapSliceIndex otherBsi) {
        this.ebM.or(otherBsi.ebM);
        for (int i = 0; i < otherBsi.bitCount(); i++) {
            this.addDigit(otherBsi.bitmaps[i], i);
        }
    }

    private void addDigit(RoaringBitmap foundSet, int i) {
        if (i >= this.bitCount()) {
            grow(this.bitCount() + 1, this.bitCount());
        }

        RoaringBitmap carry = RoaringBitmap.and(this.bitmaps[i], foundSet);
        this.bitmaps[i].xor(foundSet);
        if (carry.getCardinality() > 0) {
            if (i + 1 > this.bitCount()) {
                grow(this.bitCount() + 1, this.bitCount());
            }
            this.addDigit(carry, i + 1);
        }

    }

    public void merge(RoaringBitmapSliceIndex otherBsi) {
        if (null == otherBsi || otherBsi.ebM.isEmpty()) {
            return;
        }

        // todo whether we need this
        if (RoaringBitmap.intersects(this.ebM, otherBsi.ebM)) {
            throw new IllegalArgumentException("merge can be used only in bsiA ∩ bsiB  is null");
        }

        int bitDepth = Integer.max(this.bitCount(), otherBsi.bitCount());
        RoaringBitmap[] newBA = new RoaringBitmap[bitDepth];
        for (int i = 0; i < bitDepth; i++) {
            RoaringBitmap current = i < this.bitmaps.length ? this.bitmaps[i] : new RoaringBitmap();
            RoaringBitmap other = i < otherBsi.bitmaps.length ? otherBsi.bitmaps[i] : new RoaringBitmap();
            newBA[i] = RoaringBitmap.or(current, other);
            if (this.runOptimized || otherBsi.runOptimized) {
                newBA[i].runOptimize();
            }
        }
        this.bitmaps = newBA;
        this.ebM.or(otherBsi.ebM);
        this.runOptimized = this.runOptimized || otherBsi.runOptimized;
        this.maxValue = Integer.max(this.maxValue, otherBsi.maxValue);
        this.minValue = Integer.min(this.minValue, otherBsi.minValue);
    }

    public RoaringBitmapSliceIndex clone() {
        RoaringBitmapSliceIndex bitSliceIndex = new RoaringBitmapSliceIndex();
        bitSliceIndex.minValue = this.minValue;
        bitSliceIndex.maxValue = this.maxValue;
        bitSliceIndex.ebM = this.ebM.clone();
        RoaringBitmap[] cloneBA = new RoaringBitmap[this.bitCount()];
        for (int i = 0; i < cloneBA.length; i++) {
            cloneBA[i] = this.bitmaps[i].clone();
        }
        bitSliceIndex.bitmaps = cloneBA;
        bitSliceIndex.runOptimized = this.runOptimized;

        return bitSliceIndex;
    }

    public void runOptimize() {
        this.ebM.runOptimize();

        for (RoaringBitmap integers : this.bitmaps) {
            integers.runOptimize();
        }
        this.runOptimized = true;
    }






    public Pair<Long, Long> sum(RoaringBitmap foundSet) {
        if (null == foundSet || foundSet.isEmpty()) {
            return Pair.newPair(0L, 0L);
        }
        long count = foundSet.getLongCardinality();

        Long sum = IntStream.range(0, this.bitCount())
                .mapToLong(x -> (long) (1 << x) * RoaringBitmap.andCardinality(this.bitmaps[x], foundSet))
                .sum();

        return Pair.newPair(sum, count);
    }

    public boolean hasRunCompression() {
        return this.runOptimized;
    }

    public RoaringBitmap getExistenceBitmap() {
        return this.ebM;
    }

    @Override
    public String toString() {
        return "RoaringBitmapSliceIndex{" +
                "maxValue=" + maxValue +
                ", minValue=" + minValue +
                ", bA=" + Arrays.toString(bitmaps) +
                ", ebM=" + ebM +
                ", runOptimized=" + runOptimized +
                '}';
    }

    @VisibleForTesting
    public Map<Integer, List<Integer>> toMap() {
        Map<Integer, List<Integer>> map = Maps.newConcurrentMap();
        for (int index = 0;index < bitCount();index ++) {
            RoaringBitmap rb = bitmaps[index];
            List<Integer> vales = Lists.newArrayList();
            rb.forEach(new Consumer<Integer>() {
                @Override
                public void accept(Integer value) {
                    vales.add(value);
                }
            });
            map.put(index, vales);
        }
        return map;
    }
}
