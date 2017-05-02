package com.chinasofti.ark.bdadp.datasources.jdbc

import java.util.Properties

/**
  * Created by White on 2017/4/23.
  */
package object as400 {

  /**
    * Adds a method, `as400Jdbc`, to SQLContext that allows reading CSV data.
    */
  implicit class AS400Context(sqlContext: SQLContext) extends Serializable {
    def as400Jdbc(url: String, table: String, properties: Properties): Unit = {
      sqlContext.baseRelationToDataFrame(AS400JDBCRelation(url, table, properties)(sqlContext))
    }
  }

}
