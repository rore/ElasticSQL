package com.ElasticSQL

import com.log4p.sqldsl._
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.sort.SortBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.FilterBuilder
import org.elasticsearch.index.query.AndFilterBuilder
import org.elasticsearch.index.query.OrFilterBuilder

class ElasticSQLParser() {

	protected val _parser = new SQLParser;
	
	var fields:List[String] = null;
	var esType:String=null;
	var esQuery:QueryBuilder = null;
	var esSort:SortBuilder = null;
	
	def parse(sql:String):Unit={
		if (null == sql || sql.isEmpty()) throw new IllegalArgumentException("sql is empty");
		val query = _parser.parse(sql).getOrElse(null);
		if (null == query) throw new IllegalArgumentException("failed parsing sql");
		parse(query);
	}
	
	def parse(query:Query):Unit={
		if (null == query) throw new IllegalArgumentException("query is empty");
		getType(query);
		getFields(query);
		getQuery(query);
	}
	
	protected def getType(query:Query){
		esType = query.from.table;
	}
	
	protected def getFields(query:Query){
		val selectOp = query.operation.asInstanceOf[Select];
		if (selectOp.fields.size > 0){
			if (selectOp.fields(0) != "*"){
				fields = selectOp.fields.toList;
			}
		}
	}
	
	protected def getQuery(query:Query){
		if (!query.where.isEmpty && !query.where.get.clauses.isEmpty){
			val filter = andFilter();
			query.where.get.clauses.foreach(processClause(_,filter));
			esQuery = constantScoreQuery(filter);
		}
	}
	
	protected def processClause(clause:Clause, parent:FilterBuilder){
		var filter:FilterBuilder = clause match {
			case or:Or => processOr(or);
			case and:And => processAnd(and);
			case stringEq:StringEquals => processStringEquals(stringEq);
			//case StringEquals(field, value) => "%s = %s".format(field, quote(value))
		    //case BooleanEquals(field, value) => "%s = %s".format(field, value)
		    //case NumberEquals(field, value) => "%s = %s".format(field, value)
		    //case in:In => "%s in (%s)".format(in.field, in.values.map(quote(_)).mkString(","))
		    case _ => throw new IllegalArgumentException("Clause %s not implemented".format(clause))
		}
		addToFilter(filter,parent);
	}
	
	protected def processOr(clause:Or):FilterBuilder={
		val filter = orFilter();
		processClause(clause.lClause, filter);
		processClause(clause.rClause, filter);
		return filter;
	}

	protected def processAnd(clause:And):FilterBuilder={
		val filter = andFilter();
		processClause(clause.lClause, filter);
		processClause(clause.rClause, filter);
		return filter;
	}

	protected def processStringEquals(clause:StringEquals):FilterBuilder={
		val filter = termFilter(clause.f, clause.value);
		return filter;
	}
	
	protected def addToFilter(addFilter:FilterBuilder, parentFilter:FilterBuilder){
		parentFilter match {
			case or:OrFilterBuilder => or.add(addFilter);
			case and:AndFilterBuilder => and.add(addFilter);
			case _ => throw new IllegalArgumentException("Filter %s not supported".format(parentFilter));
		}
	}
}

object ElasticSQLParser {
	
}