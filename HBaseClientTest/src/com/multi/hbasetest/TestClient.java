package com.multi.hbasetest;

import java.io.IOException;

import com.multi.hbasetest.dao.HbaseDAO;

public class TestClient {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		HbaseDAO dao = new HbaseDAO();
//		try {
//			dao.dropTable();
//		} catch (Exception e) {}
//		
//		dao.createTableAndCf();
		dao.upsertRow("hong_2015-04-11", "ȫ�浿1", "����", "������6", "2015-04-13");
		dao.upsertRow("lee_2015-04-11", "�̸���", "����", "������s6", "2015-04-12");
		
		dao.queryTable();
	}

}
