package com.chinasofti.ark.bdadp.datasources.jdbc.as400

/**
  * Created by White on 2017/4/23.
  */
object AS400JDBCDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:as400")

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Option(JdbcType("CLOB", java.sql.Types.CLOB))
    case BooleanType => Option(JdbcType("CHAR(1)", java.sql.Types.CHAR))
    case _ => None
  }
}
