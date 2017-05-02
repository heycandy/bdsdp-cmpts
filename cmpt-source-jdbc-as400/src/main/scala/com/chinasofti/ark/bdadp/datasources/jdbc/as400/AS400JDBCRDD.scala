package com.chinasofti.ark.bdadp.datasources.jdbc.as400

import java.sql.{Date, ResultSet, Timestamp}
import java.util.Properties

/**
  * Created by White on 2017/4/23.
  */

/**
  * Data corresponding to one partition of a JDBCRDD.
  */
private case class AS400JDBCPartition(whereClause: String, idx: Int) extends Partition {
  override def index: Int = idx
}

class AS400JDBCRDD(
                    sc: SparkContext,
                    url: String,
                    table: String,
                    properties: Properties,
                    schema: StructType,
                    columns: Array[String],
                    filters: Array[Filter]
                  ) extends RDD[InternalRow](sc, Nil) {

  val getConnection = JdbcUtils.createConnectionFactory(url, properties)
  /**
    * `columns`, but as a String suitable for injection into a SQL query.
    */
  private val columnList: String = {
    val sb = new StringBuilder()
    columns.foreach(x => sb.append(",").append(x))
    if (sb.isEmpty) "1" else sb.substring(1)
  }
  /**
    * `filters`, but as a WHERE clause suitable for injection into a SQL query.
    */
  private val filterWhereClause: String = {
    val filterStrings = filters map compileFilter filter (_ != null)
    if (filterStrings.length > 0) {
      val sb = new StringBuilder("WHERE ")
      filterStrings.foreach(x => sb.append(x).append(" AND "))
      sb.substring(0, sb.length - 5)
    } else ""
  }

  /**
    * Maps a StructType to a type tag list.
    */
  def getConversions(schema: StructType): Array[JDBCConversion] =
    schema.fields.map(sf => getConversions(sf.dataType, sf.metadata))

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] =
    new Iterator[InternalRow] {
      var closed = false
      var finished = false
      var gotNext = false
      var nextValue: InternalRow = null

      context.addTaskCompletionListener { context => close() }
      val part = split.asInstanceOf[AS400JDBCPartition]
      val conn = getConnection()
      val dialect = AS400JDBCDialect

      import scala.collection.JavaConverters._

      dialect.beforeFetch(conn, properties.asScala.toMap)

      // H2's JDBC driver does not support the setSchema() method.  We pass a
      // fully-qualified table name in the SELECT statement.  I don't know how to
      // talk about a table in a completely portable way.

      val myWhereClause = getWhereClause(part)

      val sqlText = s"SELECT $columnList FROM $table $myWhereClause"
      val stmt = conn.prepareStatement(sqlText,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val fetchSize = properties.getProperty("fetchsize", "0").toInt
      stmt.setFetchSize(fetchSize)
      val rs = stmt.executeQuery()

      val conversions = getConversions(schema)
      val mutableRow = new SpecificMutableRow(schema.fields.map(x => x.dataType))

      def getNext(): InternalRow = {
        if (rs.next()) {
          var i = 0
          while (i < conversions.length) {
            val pos = i + 1
            conversions(i) match {
              case BooleanConversion => mutableRow.setBoolean(i, rs.getBoolean(pos))
              case DateConversion =>
                // DateTimeUtils.fromJavaDate does not handle null value, so we need to check it.
                val dateVal = rs.getDate(pos)
                if (dateVal != null) {
                  mutableRow.setInt(i, DateTimeUtils.fromJavaDate(dateVal))
                } else {
                  mutableRow.update(i, null)
                }
              // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
              // object returned by ResultSet.getBigDecimal is not correctly matched to the table
              // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
              // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
              // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
              // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
              // retrieve it, you will get wrong result 199.99.
              // So it is needed to set precision and scale for Decimal based on JDBC metadata.
              case DecimalConversion(p, s) =>
                val decimalVal = rs.getBigDecimal(pos)
                if (decimalVal == null) {
                  mutableRow.update(i, null)
                } else {
                  mutableRow.update(i, Decimal(decimalVal, p, s))
                }
              case DoubleConversion => mutableRow.setDouble(i, rs.getDouble(pos))
              case FloatConversion => mutableRow.setFloat(i, rs.getFloat(pos))
              case IntegerConversion => mutableRow.setInt(i, rs.getInt(pos))
              case LongConversion => mutableRow.setLong(i, rs.getLong(pos))
              // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
              case StringConversion => mutableRow.update(i, UTF8String.fromString(rs.getString(pos)))
              case TimestampConversion =>
                val t = rs.getTimestamp(pos)
                if (t != null) {
                  mutableRow.setLong(i, DateTimeUtils.fromJavaTimestamp(t))
                } else {
                  mutableRow.update(i, null)
                }
              case BinaryConversion => mutableRow.update(i, rs.getBytes(pos))
              case BinaryLongConversion =>
                val bytes = rs.getBytes(pos)
                var ans = 0L
                var j = 0
                while (j < bytes.size) {
                  ans = 256 * ans + (255 & bytes(j))
                  j = j + 1
                }
                mutableRow.setLong(i, ans)
              case ArrayConversion(elementConversion) =>
                val array = rs.getArray(pos).getArray
                if (array != null) {
                  val data = elementConversion match {
                    case TimestampConversion =>
                      array.asInstanceOf[Array[java.sql.Timestamp]].map { timestamp =>
                        nullSafeConvert(timestamp, DateTimeUtils.fromJavaTimestamp)
                      }
                    case StringConversion =>
                      array.asInstanceOf[Array[java.lang.String]]
                        .map(UTF8String.fromString)
                    case DateConversion =>
                      array.asInstanceOf[Array[java.sql.Date]].map { date =>
                        nullSafeConvert(date, DateTimeUtils.fromJavaDate)
                      }
                    case DecimalConversion(p, s) =>
                      array.asInstanceOf[Array[java.math.BigDecimal]].map { decimal =>
                        nullSafeConvert[java.math.BigDecimal](decimal, d => Decimal(d, p, s))
                      }
                    case BinaryLongConversion =>
                      throw new IllegalArgumentException(s"Unsupported array element conversion $i")
                    case _: ArrayConversion =>
                      throw new IllegalArgumentException("Nested arrays unsupported")
                    case _ => array.asInstanceOf[Array[Any]]
                  }
                  mutableRow.update(i, new GenericArrayData(data))
                } else {
                  mutableRow.update(i, null)
                }
            }
            if (rs.wasNull) mutableRow.setNullAt(i)
            i = i + 1
          }
          mutableRow
        } else {
          finished = true
          null.asInstanceOf[InternalRow]
        }
      }

      def close() {
        if (closed) return
        try {
          if (null != rs) {
            rs.close()
          }
        } catch {
          case e: Exception => logWarning("Exception closing resultset", e)
        }
        try {
          if (null != stmt) {
            stmt.close()
          }
        } catch {
          case e: Exception => logWarning("Exception closing statement", e)
        }
        try {
          if (null != conn) {
            if (!conn.isClosed && !conn.getAutoCommit) {
              try {
                conn.commit()
              } catch {
                case e: Throwable => logWarning("Exception committing transaction", e)
              }
            }
            conn.close()
          }
          logInfo("closed connection")
        } catch {
          case e: Exception => logWarning("Exception closing connection", e)
        }
        closed = true
      }

      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            nextValue = getNext()
            if (finished) {
              close()
            }
            gotNext = true
          }
        }
        !finished
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new NoSuchElementException("End of stream")
        }
        gotNext = false
        nextValue
      }
    }

  override protected def getPartitions: Array[Partition] = Array[Partition](AS400JDBCPartition(null, 0))

  private def nullSafeConvert[T](input: T, f: T => Any): Any = {
    if (input == null) {
      null
    } else {
      f(input)
    }
  }

  private def getConversions(dt: DataType, metadata: Metadata): JDBCConversion = dt match {
    case BooleanType => BooleanConversion
    case DateType => DateConversion
    case Fixed(p, s) => DecimalConversion(p, s)
    case DoubleType => DoubleConversion
    case FloatType => FloatConversion
    case IntegerType => IntegerConversion
    case LongType => if (metadata.contains("binarylong")) BinaryLongConversion else LongConversion
    case StringType => StringConversion
    case TimestampType => TimestampConversion
    case BinaryType => BinaryConversion
    case ArrayType(et, _) => ArrayConversion(getConversions(et, metadata))
    case _ => throw new IllegalArgumentException(s"Unsupported type ${dt.simpleString}")
  }

  /**
    * Turns a single Filter into a String representing a SQL expression.
    * Returns null for an unhandled filter.
    */
  private def compileFilter(f: Filter): String = f match {
    case EqualTo(attr, value) => s"$attr = ${compileValue(value)}"
    case LessThan(attr, value) => s"$attr < ${compileValue(value)}"
    case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
    case LessThanOrEqual(attr, value) => s"$attr <= ${compileValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileValue(value)}"
    case _ => null
  }

  /**
    * Converts value to SQL expression.
    */
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case _ => value
  }

  private def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  /**
    * A WHERE clause representing both `filters`, if any, and the current partition.
    */
  private def getWhereClause(part: AS400JDBCPartition): String = {
    if (part.whereClause != null && filterWhereClause.length > 0) {
      filterWhereClause + " AND " + part.whereClause
    } else if (part.whereClause != null) {
      "WHERE " + part.whereClause
    } else {
      filterWhereClause
    }
  }

  // Each JDBC-to-Catalyst conversion corresponds to a tag defined here so that
  // we don't have to potentially poke around in the Metadata once for every
  // row.
  // Is there a better way to do this?  I'd rather be using a type that
  // contains only the tags I define.
  abstract class JDBCConversion

  case class DecimalConversion(precision: Int, scale: Int) extends JDBCConversion

  case class ArrayConversion(elementConversion: JDBCConversion) extends JDBCConversion

  case object BooleanConversion extends JDBCConversion

  case object DateConversion extends JDBCConversion

  case object DoubleConversion extends JDBCConversion

  case object FloatConversion extends JDBCConversion

  case object IntegerConversion extends JDBCConversion

  case object LongConversion extends JDBCConversion

  case object BinaryLongConversion extends JDBCConversion

  case object StringConversion extends JDBCConversion

  case object TimestampConversion extends JDBCConversion

  case object BinaryConversion extends JDBCConversion

  private object Fixed {
    def unapply(t: DecimalType): Option[(Int, Int)] = Some((t.precision, t.scale))
  }

}
