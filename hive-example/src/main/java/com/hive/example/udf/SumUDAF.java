package com.hive.example.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.util.HashSet;

/**
 * 功能：SUM
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/11/27 下午12:08
 */
public class SumUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        TypeInfo[] parameters = info.getParameters();
        SumEvaluator evaluator = (SumEvaluator) getEvaluator(parameters);
        return evaluator;
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        // 只有一个参数
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        }

        TypeInfo parameter = parameters[0];
        ObjectInspector.Category category = parameter.getCategory();
        String typeName = parameter.getTypeName();
        // 只支持原生类型
        if (category != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(
                    0,
                    "Only primitive type arguments are accepted but " + typeName + " is passed."
            );
        }
        // 支持不同数据类型的实现
        switch (((PrimitiveTypeInfo) parameter).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new LongSumEvaluator();
            case TIMESTAMP:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case VARCHAR:
            case CHAR:
                return new GenericUDAFSum.GenericUDAFSumDouble();
            case DECIMAL:
                return new GenericUDAFSum.GenericUDAFSumHiveDecimal();
            case BOOLEAN:
            case DATE:
            default:
                throw new UDFArgumentTypeException(
                        0,
                        "Only numeric or string type arguments are accepted but " + typeName + " is passed."
                );
        }
    }

    /**
     * SumEvaluator
     * @param <ResultType>
     */
    public static abstract class SumEvaluator<ResultType extends Writable> extends GenericUDAFEvaluator {
        protected PrimitiveObjectInspector inputOI;
        protected PrimitiveObjectInspector outputOI;
        protected ResultType result;

        static abstract class SumAggBuffer<T> extends AbstractAggregationBuffer {
            boolean empty;
            T sum;
            HashSet<ObjectInspectorUtils.ObjectInspectorObject> uniqueObjects; // Unique rows.
        }
    }

    /**
     * LongSumEvaluator
     */
    public static class LongSumEvaluator extends SumEvaluator<LongWritable> {

        static class LongSumAggBuffer extends SumAggBuffer<Long> {
            @Override
            public int estimate() { return JavaDataModel.PRIMITIVES1 + JavaDataModel.PRIMITIVES2; }
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
            result = new LongWritable(0);
            inputOI = (PrimitiveObjectInspector) parameters[0];
            outputOI = (PrimitiveObjectInspector)ObjectInspectorUtils.getStandardObjectInspector(
                    inputOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA
            );
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            LongSumAggBuffer result = new LongSumAggBuffer();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            LongSumAggBuffer buffer = (LongSumAggBuffer) agg;
            buffer.empty = true;
            buffer.sum = 0L;
            buffer.uniqueObjects = null;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            try {
                if (parameters[0] != null) {
                    LongSumAggBuffer buffer = (LongSumAggBuffer) agg;
                    buffer.empty = false;
                    buffer.sum += PrimitiveObjectInspectorUtils.getLong(parameters[0], inputOI);
                }
            } catch (NumberFormatException e) {

            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            LongSumAggBuffer buffer = (LongSumAggBuffer) agg;
            if (buffer.empty) {
                return null;
            }
            result.set(buffer.sum);
            return result;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                LongSumAggBuffer buffer = (LongSumAggBuffer) agg;
                buffer.empty = false;
                buffer.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            LongSumAggBuffer buffer = (LongSumAggBuffer) agg;
            if (buffer.empty) {
                return null;
            }
            result.set(buffer.sum);
            return result;
        }
    }
}
