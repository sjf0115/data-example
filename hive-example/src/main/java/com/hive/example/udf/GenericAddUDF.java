package com.hive.example.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 功能：Generic Add UDF
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/6 下午11:09
 */
public class GenericAddUDF extends GenericUDF {
    private IntObjectInspector intOI;
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 参数个数校验
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function 'Add' only accepts 2 argument, but got " + arguments.length);
        }
        ObjectInspector a1 = arguments[0];
        ObjectInspector a2 = arguments[1];
        // 参数类型校验
        if (!(a1 instanceof IntObjectInspector)) {
            throw new UDFArgumentException("The first argument of function must be a int");
        }
        if (!(a2 instanceof IntObjectInspector)) {
            throw new UDFArgumentException("The second argument of function must be a int");
        }
        this.intOI = (IntObjectInspector) a1;
        this.intOI = (IntObjectInspector) a2;
        // 返回值类型
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object o1 = deferredObjects[0].get();
        Object o2 = deferredObjects[1].get();
        // 利用 ObjectInspector 从 DeferredObject[] 中获取元素值
        int a1 = (int) this.intOI.getPrimitiveJavaObject(o1);
        int a2 = (int) this.intOI.getPrimitiveJavaObject(o2);
        return new Integer(a1 + a2);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "custom_add(T, T)";
    }
}
