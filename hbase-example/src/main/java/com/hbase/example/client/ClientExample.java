package com.hbase.example.client;

import com.hbase.example.utils.HBaseConn;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Client API
 * Created by wy on 2020/1/12.
 */
public class ClientExample {
    /**
     * 插入数据
     * @param tableName
     * @param cf
     * @param column
     * @param rowKey
     * @param value
     * @throws IOException
     */
    public static void putRow(String tableName, String cf, String column, String rowKey, String value) throws IOException {
        Connection connection = HBaseConn.create();
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 批量插入数据
     * @throws IOException
     */
    public static void putRowBatch() throws IOException {
        Connection connection = HBaseConn.create();
        Table table = connection.getTable(TableName.valueOf("user"));

        List<String> rowKeyList = Lists.newArrayList("Tom", "Kary", "Ford");
        List<String> ageList = Lists.newArrayList("15", "43", "21");
        List<Put> putList = Lists.newArrayList();
        for (int index = 0;index < rowKeyList.size();index ++) {
            String rowKey = rowKeyList.get(index);
            String age = ageList.get(index);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age));
            putList.add(put);
        }
        table.put(putList);
        table.close();
    }

    /**
     * 读取数据
     * @param tableName
     * @param cf
     * @param column
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String cf, String column, String rowKey) throws IOException {

        Connection connection = HBaseConn.create();
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(column));
        System.out.println(Bytes.toString(value));

    }

    /**
     * 批量获取数据
     * @throws IOException
     */
    public static void getRowBatch() throws IOException {
        Connection connection = HBaseConn.create();
        Table table = connection.getTable(TableName.valueOf("user"));

        List<String> rowKeyList = Lists.newArrayList("lucy", "lily");
        List<Get> getList = Lists.newArrayList();
        for (int index = 0;index < rowKeyList.size();index ++) {
            String rowKey = rowKeyList.get(index);
            Get get = new Get(Bytes.toBytes(rowKey));
            getList.add(get);
        }
        Result[] results = table.get(getList);
        for (Result result : results) {
            byte[] ageValue = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
            System.out.println(Bytes.toString(ageValue));
        }
        table.close();
    }

    /**
     * 删除指定行
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteRow(String tableName, String rowKey) throws IOException {
        Connection connection = HBaseConn.create();
        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    /**
     * 删除指定行指定列
     * @param tableName
     * @param cf
     * @param column
     * @param rowKey
     * @throws IOException
     */
    public static void deleteColumn(String tableName, String cf, String column, String rowKey) throws IOException {
        Connection connection = HBaseConn.create();
        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column));
        table.delete(delete);
    }

    /**
     * 批量删除数据
     * @throws IOException
     */
    public static void deleteRowBatch() throws IOException {
        Connection connection = HBaseConn.create();
        Table table = connection.getTable(TableName.valueOf("user"));

        List<String> rowKeyList = Lists.newArrayList("lucy", "lily");
        List<Delete> deleteList = Lists.newArrayList();
        for (int index = 0;index < rowKeyList.size();index ++) {
            String rowKey = rowKeyList.get(index);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }


    /**
     * 检索数据
     * @param tableName
     * @throws IOException
     */
    public static void scan(String tableName) throws IOException {
        Connection connection = HBaseConn.create();
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            byte[] rowBytes = result.getRow();
            byte[] sexBytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"));
            byte[] ageBytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
            byte[] addressBytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("address"));

            String rowKey = Bytes.toString(rowBytes);
            String sex = Bytes.toString(sexBytes);
            String age = Bytes.toString(ageBytes);
            String address = Bytes.toString(addressBytes);

            System.out.println("rowKey: " + rowKey + ", cf: info, sex: " + sex + ", age: " + age + ", address: " + address);
        }
        scanner.close();
    }

    public static void main(String[] args) throws IOException {
        String tableName = "user";
        String cf = "info";
        String column = "age";

        String rowKey = "lily";
        String value = "24";
        // putRow(tableName, cf, column, rowKey, value);
        // getRow(tableName, cf, column, rowKey);
        // getRowBatch();
        // deleteColumn(tableName, cf, column, rowKey);
        // deleteRow(tableName, rowKey);
        deleteRowBatch();
        // scan(tableName);
    }
}
