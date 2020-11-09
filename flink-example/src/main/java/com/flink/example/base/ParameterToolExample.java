package com.flink.example.base;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * ParameterTool
 * Created by wy on 2020/10/31.
 */
public class ParameterToolExample {

    public void local() {
        try {
            String propertiesFile = "/Users/wy/study/dev.properties";
            ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);
            String user = parameter.get("user");
            System.out.println(user);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
