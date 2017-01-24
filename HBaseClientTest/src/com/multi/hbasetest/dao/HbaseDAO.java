package com.multi.hbasetest.dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDAO {
	
	private String tblName = "orders";
	private String cf1 = "client";
	private String cf2 = "product";
	
	private Configuration config = null;

	public HbaseDAO() {
		config = HBaseConfiguration.create();
		config.clear();
		config.set("hbase.master", "s1.test.com");
		config.set("hbase.zookeeper.quorum", "s1.test.com");
		config.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public void dropTable() throws IOException {
		HBaseAdmin hadmin = new HBaseAdmin(config);
		hadmin.disableTable(tblName);
		hadmin.deleteTable(tblName);
	}
	
	public void createTableAndCf() throws IOException {
		HBaseAdmin hadmin = new HBaseAdmin(config);
		HTableDescriptor desc = new HTableDescriptor(tblName);
		
		HColumnDescriptor meta1 = new HColumnDescriptor(cf1.getBytes());
		desc.addFamily(meta1);
		HColumnDescriptor meta2 = new HColumnDescriptor(cf2.getBytes());
		desc.addFamily(meta2);
		
		hadmin.createTable(desc);
        System.out.println("테이블 생성 완료");
	}
	
	public void upsertRow(String rowKey, String name, String address, String title, String delivery) throws IOException {
		 HTable table =new HTable(config, tblName);
		 Put put =new Put(Bytes.toBytes(rowKey));
		 put.add(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(name));
		 put.add(Bytes.toBytes(cf1), Bytes.toBytes("address"), Bytes.toBytes(address));
		 put.add(Bytes.toBytes(cf2), Bytes.toBytes("title"), Bytes.toBytes(title));
		 put.add(Bytes.toBytes(cf2), Bytes.toBytes("delivery"), Bytes.toBytes(delivery));
		 table.put(put);
		 table.close();
		 System.out.println(rowKey + " 추가 완료");
	}
	
	public void queryTable() throws IOException {
		HTable table =new HTable(config, tblName);
		Scan s =new Scan();
        ResultScanner rs = table.getScanner(s);
        for (Result r : rs) {
            for (KeyValue kv : r.raw()) {
                System.out.println("row:" + new String(kv.getRow(), "utf-8") +"");
                System.out.println("family:" + new String(kv.getFamily(), "utf-8") +":");
                System.out.println("qualifier:" + new String(kv.getQualifier(), "utf-8") +"");
                System.out.println("value:" + new String(kv.getValue(), "utf-8"));
                System.out.println("timestamp:" + kv.getTimestamp() +"");
                System.out.println("-------------------------------------------");
            }
        }
        table.close();
        System.out.println("조회 완료");
	}
}





