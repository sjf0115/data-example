package com.hive.example.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;

/**
 * 功能：AVG 基于 AbstractGenericUDAFResolver 实现
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/11/28 下午9:39
 */
@Description(name = "avg", value = "_FUNC_(x) - Returns the mean of a set of numbers")
public class AbstractGenericAverageUDAF extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(AbstractGenericAverageUDAF.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return getEvaluator(info.getParameters());
    }

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
                return new AverageUDAFEvaluator();
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
    public static class AverageUDAFEvaluator extends GenericUDAFEvaluator {

        // Iterate 输入
        private PrimitiveObjectInspector inputOI;
        // Merge 输入
        private StructObjectInspector structOI;
        private LongObjectInspector countFieldOI;
        private DoubleObjectInspector sumFieldOI;
        private StructField countField;
        private StructField sumField;

        // TerminatePartial 输出
        private Object[] partialResult;
        // Terminate 输出
        private DoubleWritable result;

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
            // 初始化输入参数
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                // 原生类型
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                // Struct
                structOI = (StructObjectInspector) parameters[0];
                countField = structOI.getStructFieldRef("count");
                sumField = structOI.getStructFieldRef("sum");
                countFieldOI = (LongObjectInspector) countField.getFieldObjectInspector();
                sumFieldOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
            }
            // 初始化输出
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                // 输出类型
                partialResult = new Object[2];
                partialResult[0] = new LongWritable(0);
                partialResult[1] = new DoubleWritable(0);
                // 输出OI
                // 字段类型
                ArrayList<ObjectInspector> structFieldOIs = new ArrayList<ObjectInspector>();
                structFieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                structFieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                // 字段名称
                ArrayList<String> structFieldNames = new ArrayList<String>();
                structFieldNames.add("count");
                structFieldNames.add("sum");
                return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldOIs);
            } else {
                result = new DoubleWritable(0);
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }
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
                throw new HiveException("iterate exception", e);
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
            Object partialCount = structOI.getStructFieldData(partial, countField);
            Object partialSum = structOI.getStructFieldData(partial, sumField);
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
