package com.hbase.example.admin;

import com.hbase.example.utils.HBaseConn;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Admin API
 * Created by wy on 2020/1/12.
 */
public class AdminExample {

    /**
     * 创建表
     * @param name
     * @param columnFamilies
     * @throws IOException
     */
    public static void create(String name, String... columnFamilies) throws IOException {

        Connection connection = HBaseConn.create();
        Admin admin = connection.getAdmin();

        TableName tableName =  TableName.valueOf(name);

        if (admin.tableExists(tableName)) {
            System.out.println("table " + tableName + " already exists");
            return;
        }
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        for (String cf : columnFamilies) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf));
        }
        admin.createTable(builder.build());
        System.out.println("create table " + tableName + " success");

        /*
        // before 2.0.0
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(name));
        for (String cf : columnFamilies) {
            tableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        admin.createTable(tableDescriptor);*/

    }

    /**
     * 判断表是否存在
     * @param name
     * @return
     * @throws IOException
     */
    public static boolean exists(String name) throws IOException {
        Connection connection = HBaseConn.create();
        Admin admin = connection.getAdmin();
        boolean result = admin.tableExists(TableName.valueOf(name));
        return result;
    }

    /**
     * 禁用表
     * @param name
     * @throws IOException
     */
    public static void disable(String name) throws IOException {
        Connection connection = HBaseConn.create();
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(name);

        if (!admin.tableExists(tableName)) {
            System.out.println("table " + tableName + " not exists");
            return;
        }

        boolean isDisabled = admin.isTableDisabled(tableName);
        if (!isDisabled) {
            System.out.println("disable table " + name);
            admin.disableTable(tableName);
        }
    }

    /**
     * 启用表
     * @param name
     * @throws IOException
     */
    public static void enable(String name) throws IOException {
        Connection connection = HBaseConn.create();
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(name);

        if (!admin.tableExists(tableName)) {
            System.out.println("table " + tableName + " not exists");
            return;
        }

        boolean isEnabled = admin.isTableEnabled(tableName);
        if (!isEnabled) {
            System.out.println("enable table " + name);
            admin.enableTable(tableName);
        }
    }

    /**
     * 删除表
     * @param name
     * @throws IOException
     */
    public static void delete(String name) throws IOException {
        Connection connection = HBaseConn.create();
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(name);

        if (!admin.tableExists(tableName)) {
            System.out.println("table " + tableName + " not exists");
            return;
        }

        // 禁用表
        admin.disableTable(tableName);
        // 删除表
        admin.deleteTable(tableName);
        System.out.println("delete table " + name);
    }

    /**
     * 添加列族
     * @param name
     * @param cf
     * @throws IOException
     */
    public static void addColumnFamily(String name, String cf) throws IOException {
        Connection connection = HBaseConn.create();
        Admin admin = connection.getAdmin();

        TableName tableName =  TableName.valueOf(name);

        if (!admin.tableExists(tableName)) {
            System.out.println("table " + tableName + " not exists");
            return;
        }

        ColumnFamilyDescriptor desc = ColumnFamilyDescriptorBuilder.of(cf);
        admin.addColumnFamily(tableName, desc);
        System.out.println("add column family " + cf);

    }

    /**
     * 删除列族
     * @param name
     * @param cf
     * @throws IOException
     */
    public static void deleteColumnFamily(String name, String cf) throws IOException {
        Connection connection = HBaseConn.create();
        Admin admin = connection.getAdmin();

        TableName tableName =  TableName.valueOf(name);

        if (!admin.tableExists(tableName)) {
            System.out.println("table " + tableName + " not exists");
            return;
        }

        admin.deleteColumnFamily(tableName, cf.getBytes());
        System.out.println("delete column family " + cf);

    }

    /**
     * 获取Admin
     * @return
     * @throws IOException
     */
    public static Admin getAdmin () throws IOException {
        Connection connection = HBaseConn.create();
        Admin admin = connection.getAdmin();
        return admin;
    }

    /**
     * 停止HBase
     * @throws IOException
     */
    public static void shutdown () throws IOException {
        Connection connection = HBaseConn.create();
        Admin admin = connection.getAdmin();
        admin.shutdown();
    }

    public static void main(String[] args) throws IOException {

        String name = "user";
        String cf = "info";

        AdminExample.create(name, cf);
    }
}
