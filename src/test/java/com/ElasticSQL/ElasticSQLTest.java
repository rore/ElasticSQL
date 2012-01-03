package com.ElasticSQL;

import static org.junit.Assert.*;

import org.junit.Test;

public class ElasticSQLTest {

	@Test
	public void test() {
		String sql = "select * from type where id=\"3\"";
		ElasticSQLParser parser = new ElasticSQLParser();
		parser.parse(sql);
	}

	@Test
	public void test2() {
		//String sql = "select * from type where id in (\"1\",\"2\",\"3\")";
		String sql = "select * from type where id in (1, 2, 3)";
		ElasticSQLParser parser = new ElasticSQLParser();
		parser.parse(sql);
	}

}
