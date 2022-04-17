package com.flink.example.stream.base.typeInformation.hints;

import com.flink.example.bean.Stu;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 功能：ResultTypeQueryable 示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/19 下午3:50
 */
public class ResultTypeQueryableExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.socketTextStream("localhost", 9100, "\n");

        DataStream<Stu> result = source
                .map(new ResultTypeMapFunction());
        result.print();

        env.execute("ResultTypeQueryableExample");
    }

    /**
     * 实现 ResultTypeQueryable 接口
     */
    public static class ResultTypeMapFunction implements MapFunction<String, Stu>, ResultTypeQueryable {
        @Override
        public Stu map(String value) throws Exception {
            String[] params = value.split(",");
            String name = params[0];
            int age = Integer.parseInt(params[1]);
            return new Stu(name, age);
        }

        /**
         * 返回结果数据类型的 TypeInformation
         */
        @Override
        public TypeInformation getProducedType() {
            return Types.POJO(Stu.class);
        }
    }
}
