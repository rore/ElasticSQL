package com.ElasticSQL

import com.log4p.sqldsl._
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.sort.SortBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.FilterBuilder
import org.elasticsearch.index.query.AndFilterBuilder
import org.elasticsearch.index.query.OrFilterBuilder
import org.elasticsearch.search.sort.SortBuilders._
import org.elasticsearch.search.sort.SortBuilder
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.search.sort.FieldSortBuilder

class ElasticSQLParser() {

	protected val _parser = new SQLParser;
	
	var fields:List[String] = null;
	var esType:String=null;
	var esQuery:QueryBuilder = null;
	var esSort:SortBuilder = null;
	var limit:Int = 0;
	var start:Int = 0;
	
	def parse(sql:String):Unit={
		if (null == sql || sql.isEmpty()) throw new IllegalArgumentException("sql is empty");
		val query = _parser.parse(sql).getOrElse(null);
		if (null == query) throw new IllegalArgumentException("failed parsing sql: " + _parser.lastError.getOrElse(null));
		parse(query);
	}
	
	def parse(query:Query):Unit={
		if (null == query) throw new IllegalArgumentException("query is empty");
		getType(query);
		getFields(query);
		getQuery(query);
		getOrder(query);
		getLimit(query);
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
	
	protected def getOrder(query:Query){
		esSort = query.order match {
			case Some(direction) => direction match {
				case Asc(field) => fieldSort(field).order(SortOrder.ASC);
				case Desc(field) => fieldSort(field).order(SortOrder.DESC);
			}
			case None => null;
		}
	}
	
	protected def getLimit(query:Query){
		limit = query.limit match {
			case Some(l) => l.limit;
			case None => 0;
		}
	}
	
	protected def processClause(clause:Clause, parent:FilterBuilder){
		var filter:FilterBuilder = clause match {
			case or:Or => processOr(or);
			case and:And => processAnd(and);
			case stringEq:StringEquals => termFilter(stringEq.field, stringEq.value);
			case boolEq:BooleanEquals => termFilter(boolEq.field, boolEq.value);
			case numEq:NumberEquals => termFilter(numEq.field, numEq.value);
		    case in:InString => termsFilter(in.field, in.values:_*);
		    case in:InNumber => termsFilter(in.field, in.values:_*);
		    case gts:GTString => rangeFilter(gts.field).gt(gts.value);
		    case gtes:GTEString => rangeFilter(gtes.field).gte(gtes.value);
		    case lts:LTString => rangeFilter(lts.field).lt(lts.value);
		    case ltes:LTEString => rangeFilter(ltes.field).lte(ltes.value);
		    case gtn:GTNumber => rangeFilter(gtn.field).gt(gtn.value);
		    case gten:GTENumber => rangeFilter(gten.field).gte(gten.value);
		    case ltn:LTNumber => rangeFilter(ltn.field).lt(ltn.value);
		    case lten:LTENumber => rangeFilter(lten.field).lte(lten.value);
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