package com.flink.example.stream.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction Example
 * Created by wy on 2021/2/28.
 */
public class ProcessFunctionExample {


    public class CountWithTimeoutFunction2 extends ProcessFunction<Tuple, Tuple2<String, Long>> {

        @Override
        public void processElement(Tuple value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

        }
    }

}
