package com.flink.example.stream.source.datagen;

import com.common.example.bean.Person;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

/**
 * 功能：Person Generator
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/3/19 下午4:03
 */
public class PersonGenerator extends RandomGenerator<Person> {
    @Override
    public Person next() {
        return new Person(
                StringUtils.upperCase(random.nextSecureHexString(8)),
                random.nextInt(10, 100)
        );
    }
}
