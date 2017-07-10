package com.chinasofti.ark.bdadp.component

import java.sql.DriverManager
import java.util.Properties

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, SparkSourceAdapter}
import com.chinasofti.ark.bdadp.util.common.StringUtils
import com.ibm.as400.access.AS400JDBCDriver
import org.slf4j.Logger

/**
 * Created by wumin on 2017/4/5.
 */
class AS400Source(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log) with Configureable with
  SparkSourceAdapter[SparkData] {

  val properties = new Properties

  var hostName: String = _
  var port: String = _
  var dbName: String = _
  var library: String = _

  var driver: String = _
  var url: String = _
  var file: String = _

  override def call(): StringData = Builder.build("")

  override def configure(props: ComponentProps): Unit = {
    DriverManager.registerDriver(new AS400JDBCDriver)

    hostName = props.getString("hostName")
    port = props.getString("port")
    dbName = props.getString("dbName")
    library = props.getString("library")

    driver = "com.ibm.as400.access.AS400JDBCDriver"
    url = this.getUrl(props)
    file = props.getString("file")

    StringUtils.assertIsBlank(hostName, dbName, library, file)

    properties.put("user", props.getString("userName"))
    properties.put("password", props.getString("passWord"))
    properties.put("decoding.column", props.getString("decodingColumn"))
    properties.put("charset.column", props.getString("charsetColumn", "ibm935"))

  }

  private def getUrl(props: ComponentProps): String = {
    // "jdbc:as400://host/library;database name=DBName;prompt=false;naming=sql;data truncation=false;translate binary=true"

    "jdbc:as400://" + hostName + (if (port != null && port.nonEmpty) ":" + port else "") + "/" +
    library + ";database name=" + dbName +
    ";prompt=false;naming=sql;data truncation=false;translate binary=true"

  }

  override def spark(sparkScenarioOptions: SparkScenarioOptions): SparkData = {
    val df = sparkScenarioOptions.sqlContext().read
        .option("driver", driver)
        .jdbc(url, file, properties)
    Builder.build(df)
  }
}


