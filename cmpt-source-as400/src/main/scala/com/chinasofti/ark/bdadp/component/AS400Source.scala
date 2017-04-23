package com.chinasofti.ark.bdadp.component

import java.sql.DriverManager
import java.util.Properties

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, SparkSourceAdapter}
import com.chinasofti.ark.bdadp.util.common.StringUtils
import com.ibm.as400.access.{AS400JDBCDriver}

import org.slf4j.Logger

/**
 * Created by wumin on 2017/4/5.
 */
class AS400Source(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log) with Configureable with
  SparkSourceAdapter[SparkData] {

  var hostName = ""
  var port = ""
  var dbName = ""
  var userName = ""
  var passWord = ""
  var library = ""
  var file = ""
  var url = ""
  var driver = ""

  val properties = new Properties

  override def call(): StringData = {
    return null;
  }

  override def configure(props: ComponentProps): Unit = {

    DriverManager.registerDriver(new AS400JDBCDriver)

    this.hostName = props.getString("hostName")
    this.port = props.getString("port")
    this.dbName = props.getString("dbName")
    this.library = props.getString("library")
    this.driver = "com.ibm.as400.access.AS400JDBCDriver"
    this.file = props.getString("file")

    info("jdbcUrl: " + this.getUrl(props))
    this.url = this.getUrl(props)

    StringUtils.assertIsBlank(hostName, dbName, userName, passWord, library, file)
    this.properties.put("user", props.getString("userName"))
    this.properties.put("password", props.getString("passWord"))

  }

  override def spark(sparkScenarioOptions: SparkScenarioOptions): SparkData = {
    val df = sparkScenarioOptions.sqlContext().read.option("driver", driver)
      .jdbc(url, file, properties)
    Builder.build(df)
  }

  private def getUrl(props: ComponentProps): String = {
    // "jdbc:as400://host/library;database name=DBName;naming=sql;prompt=false;translate binary=true"

    "jdbc:as400://" + hostName + (if (port != null && port.nonEmpty) ":" + port else "") + "/" +
      library + ";database name=" + dbName + ";naming=sql;prompt=false;translate binary=true"

  }
}


