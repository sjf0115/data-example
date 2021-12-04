package com.hive.example.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

/**
 * 功能：AVG 函数 基于 GenericUDAFResolver 实现
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/12/4 下午5:21
 */
public class GenericAverageUDAF implements GenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericAverageUDAF.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case VARCHAR:
            case CHAR:
            case TIMESTAMP:
                return new GenericAverageUDAFEvaluator();
            case DECIMAL:
            case BOOLEAN:
            case DATE:
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric or string type arguments are accepted but "
                                + parameters[0].getTypeName() + " is passed.");
        }
    }

    /**
     * AverageUDAFEvaluator
     */
    public static class GenericAverageUDAFEvaluator extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector inputOI;
        private PrimitiveObjectInspector outputOI;
        private DoubleWritable result;

        private StructObjectInspector soi;
        private LongObjectInspector countFieldOI;
        private DoubleObjectInspector sumFieldOI;
        private StructField countField;
        private StructField sumField;

        Object[] partialResult;

        // 中间结果
        static class AverageAggBuffer implements AggregationBuffer {
            long count;
            double sum;
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {

            LOG.info("[INFO] AverageUDAFEvaluator init");

            assert (parameters.length == 1);
            super.init(mode, parameters);
            result = new DoubleWritable(0);
            inputOI = (PrimitiveObjectInspector) parameters[0];
            outputOI = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(
                    inputOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA
            );
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            LOG.info("[INFO] AverageUDAFEvaluator getNewAggregationBuffer");
            AverageAggBuffer buffer = new AverageAggBuffer();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            LOG.info("[INFO] AverageUDAFEvaluator reset");
            AverageAggBuffer buffer = (AverageAggBuffer) agg;
            buffer.count = 0L;
            buffer.sum = 0.0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            LOG.info("[INFO] AverageUDAFEvaluator iterate");
            assert (parameters.length == 1);
            try {
                if (parameters[0] != null) {
                    AverageAggBuffer buffer = (AverageAggBuffer) agg;
                    buffer.count ++;
                    buffer.sum += PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputOI);
                }
            } catch (NumberFormatException e) {
            }
        }

        // 输出部分聚合结果
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            LOG.info("[INFO] AverageUDAFEvaluator terminatePartial");
            AverageAggBuffer buffer = (AverageAggBuffer) agg;
            ((LongWritable) partialResult[0]).set(buffer.count);
            ((DoubleWritable) partialResult[1]).set(buffer.sum);
            return partialResult;
        }

        // 合并
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            LOG.info("[INFO] AverageUDAFEvaluator merge");
            if (partial == null) {
                return;
            }
            AverageAggBuffer buffer = (AverageAggBuffer) agg;
            // 通过 StandardStructObjectInspector 实例，分解出 partial 数组元素值
            Object partialCount = soi.getStructFieldData(partial, countField);
            Object partialSum = soi.getStructFieldData(partial, sumField);
            // 通过基本数据类型的 OI 实例解析 Object 的值
            buffer.count += countFieldOI.get(partialCount);
            buffer.sum += sumFieldOI.get(partialSum);
        }

        // 输出最终聚合结果
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            LOG.info("[INFO] AverageUDAFEvaluator terminate");
            AverageAggBuffer buffer = (AverageAggBuffer) agg;
            if (buffer.count == 0) {
                return null;
            }
            result.set(buffer.sum / buffer.count);
            return result;
        }
    }
}
