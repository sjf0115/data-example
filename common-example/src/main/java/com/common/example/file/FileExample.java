package com.common.example.file;

import com.common.example.bean.UserBehavior;
import com.common.example.utils.DateUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;

/**
 * 功能：File 简单示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/3 下午10:31
 */
public class FileExample {

    private static final Gson gson = new GsonBuilder().create();

    public static void main(String[] args) {
        String inputFileName = "/Users/wy/Downloads/UserBehavior.csv";
        String outputFileName = "/Users/wy/study/code/data-example/kafka-example/src/main/resources/user_behavior.txt";
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            FileReader fr = new FileReader(inputFileName);
            FileWriter fw = new FileWriter(outputFileName);
            reader = new BufferedReader(fr);
            writer = new BufferedWriter(fw);
            int index = 0;
            while (reader.ready()) {
                String line = reader.readLine();
                String[] params = line.split(",");
                Long uid = Long.parseLong(params[0]);
                Long pid = Long.parseLong(params[1]);
                Long cid = Long.parseLong(params[2]);
                String type = params[3];
                Long timestamp = Long.parseLong(params[4])*1000;
                String date = DateUtil.timeStamp2Date(timestamp);
                String dt = date.substring(0, 10);
                if (dt.equals("2017-11-27") || dt.equals("2017-11-28")) {
                    String json = gson.toJson(new UserBehavior(uid, pid, cid, type, timestamp, date));
                    System.out.println("Index: " + index + ", " + json + ", " + date);
                    writer.write(json);
                    writer.newLine();
                    writer.flush();
                    index ++;
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
