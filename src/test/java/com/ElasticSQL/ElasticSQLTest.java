package com.ElasticSQL;

import static org.junit.Assert.*;

import org.junit.Test;

public class ElasticSQLTest {

	@Test
	public void test() {
		//String sql = "select * from type where (_id in (\"a\",\"b\") and val > 3) or (_id = \"c\" and val < 2)";
		String sql = "select * from \"posts-1211\" where \"clpInfo.art.untouched\"=\"Radiohead\" and \"pt.$date\" < \"2011-12-20T00:00\"";
		ElasticSQLParser parser = new ElasticSQLParser();
		parser.parse(sql);
	}

	@Test
	public void test2() {
		String sql = "select * from type where id in (1,2,3)";
		//String sql = "select * from type where id > \"3\" order by name asc limit 5";
		ElasticSQLParser parser = new ElasticSQLParser();
		parser.parse(sql);
	}

}
