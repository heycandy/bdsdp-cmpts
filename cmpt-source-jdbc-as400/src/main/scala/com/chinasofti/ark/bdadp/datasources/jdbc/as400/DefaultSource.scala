package com.chinasofti.ark.bdadp.datasources.jdbc.as400

import java.util.Properties

/**
  * Created by White on 2017/4/23.
  */
class DefaultSource extends RelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val properties = new Properties()

    parameters.foreach {
      case (k, v) => properties.setProperty(k, v)
    }

    val url = properties.getProperty("url")
    val table = properties.getProperty("dbtable")

    AS400JDBCRelation(url, table, properties)(sqlContext)
  }
}
