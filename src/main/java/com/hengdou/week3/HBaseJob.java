package com.hengdou.week3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

public class HBaseJob {

    public static void main(String[] args) throws IOException {
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("doudou:student");
        String colFamily = "info";
        String colFamily2 = "score";
        String name = "doudou";

        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily2);
            hTableDescriptor.addFamily(hColumnDescriptor);
            hTableDescriptor.addFamily(hColumnDescriptor2);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        // 插入数据
        Put put = new Put(Bytes.toBytes(name)); // row key
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("student_id"), Bytes.toBytes("G20220735020194"));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes("80"));
        put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes("80"));
        conn.getTable(tableName).put(put);
        System.out.println("Data insert success");

        // 查看数据
        Get get = new Get(Bytes.toBytes(name));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(name));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}