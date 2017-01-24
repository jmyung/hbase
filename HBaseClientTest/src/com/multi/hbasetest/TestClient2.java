package com.multi.hbasetest;

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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class TestClient2 {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String tblName = "orders";
		String cf1 = "client";
		String cf2 = "product";
		
		Configuration config = HBaseConfiguration.create();
		config.clear();
		config.set("hbase.master", "s1.test.com");
		config.set("hbase.zookeeper.quorum", "s1.test.com");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		
		//---Query Data
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
