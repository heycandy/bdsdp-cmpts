package com.chinasofti.ark.bdadp.datasources.jdbc.as400

import java.sql.{ResultSetMetaData, SQLException}
import java.util.Properties

import scala.math.min

/**
  * Created by White on 2017/4/23.
  */
object AS400JDBCRelation {

  /**
    * Takes a (schema, table) specification and returns the table's Catalyst
    * schema.
    *
    * @param url   - The JDBC url to fetch information from.
    * @param table - The table name of the desired table.  This may also be a
    *              SQL query wrapped in parentheses.
    * @return A StructType giving the table's Catalyst schema.
    * @throws SQLException if the table specification is garbage.
    * @throws SQLException if the table contains an unsupported type.
    */
  def resolveTable(url: String, table: String, properties: Properties): StructType = {
    val conn = JdbcUtils.createConnectionFactory(url, properties)()
    val dialect = AS400JDBCDialect
    try {
      val statement = conn.prepareStatement(s"SELECT * FROM $table WHERE 1=0")
      try {
        val rs = statement.executeQuery()
        try {
          val rsmd = rs.getMetaData
          val ncols = rsmd.getColumnCount
          val fields = new Array[StructField](ncols)
          var i = 0
          while (i < ncols) {
            val columnName = rsmd.getColumnLabel(i + 1)
            val dataType = rsmd.getColumnType(i + 1)
            val typeName = rsmd.getColumnTypeName(i + 1)
            val fieldSize = rsmd.getPrecision(i + 1)
            val fieldScale = rsmd.getScale(i + 1)
            val isSigned = rsmd.isSigned(i + 1)
            val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
            val metadata = new MetadataBuilder().putString("name", columnName)
            val columnType =
              dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
                getCatalystType(dataType, fieldSize, fieldScale, isSigned))
            fields(i) = StructField(columnName, columnType, nullable, metadata.build())
            i = i + 1
          }
          return new StructType(fields)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }

    throw new RuntimeException("This line is unreachable.")
  }

  /**
    * Maps a JDBC type to a Catalyst type.  This function is called only when
    * the JdbcDialect class corresponding to your database driver returns null.
    *
    * @param sqlType - A field of java.sql.Types
    * @return The Catalyst type corresponding to sqlType.
    */
  private def getCatalystType(
                               sqlType: Int,
                               precision: Int,
                               scale: Int,
                               signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY => null
      case java.sql.Types.BIGINT => if (signed) {
        LongType
      } else {
        DecimalType(20, 0)
      }
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
      case java.sql.Types.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER => if (signed) {
        IntegerType
      } else {
        LongType
      }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
      case java.sql.Types.NUMERIC => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER => null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.ROWID => LongType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ => null
      // scalastyle:on
    }

    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }
}

case class AS400JDBCRelation(url: String, table: String, properties: Properties)
                            (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  override val schema: StructType = AS400JDBCRelation.resolveTable(url, table, properties)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new AS400JDBCRDD(
      sqlContext.sparkContext,
      url,
      table,
      properties,
      schema,
      requiredColumns,
      filters
    ).asInstanceOf[RDD[Row]]
  }

}
