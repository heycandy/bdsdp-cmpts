package com.chinasofti.ark.bdadp.component

import java.sql.Connection
import java.util.Properties

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.SparkData
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.slf4j.Logger

/**
 * Created by water on 2017.4.23
 */
class OracleSink(id: String, name: String, log: Logger)
  extends SinkComponent[SparkData](id, name, log) with Configureable with
  SparkSinkAdapter[SparkData] {

  var conUrl: String = _
  var table: String = _
  var userName: String = _
  var passWord: String = _
  var numPartitions: Int = 0
  var mode: String = _
  var truncate: String = _

  var driver: String = _
  var properties = new Properties

  override def configure(componentProps: ComponentProps): Unit = {
    conUrl = componentProps.getString("conUrl")
    table = componentProps.getString("table")
    userName = componentProps.getString("userName")
    passWord = componentProps.getString("passWord")
    numPartitions = componentProps.getInt("numPartitions", 8)
    mode = componentProps.getString("mode", "append")
    truncate = componentProps.getString("truncate")

    StringUtils.assertIsBlank(conUrl, table, userName, passWord)

    driver = "oracle.jdbc.OracleDriver"
    properties.put("user", userName)
    properties.put("password", passWord)

  }

  override def apply(inputT: SparkData): Unit = {
    if (truncate.equals("true")) {
      val conn = JdbcUtils.createConnectionFactory(conUrl, properties)()
      truncateTable(conn, table)
    }

    inputT.getRawData.repartition(numPartitions).write
        .option("driver", driver)
        .option("truncate", truncate)
        .mode(mode)
        .jdbc(conUrl, table, properties)

  }

  /**
   * Truncate a table from the JDBC database.
   */
  def truncateTable(conn: Connection, table: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.executeUpdate(s"TRUNCATE TABLE $table")
    } finally {
      statement.close()
      conn.close()
    }
  }
}
