package com.chinasofti.ark.bdadp.component

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, SparkSourceAdapter}
import org.slf4j.Logger

/**
  * Created by White on 2017/4/23
  */
class AS400JDBCSource(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log) with Configureable with
    SparkSourceAdapter[SparkData] {

  val DRIVER_KEY = "driver"
  val SYSTEM_NAME_KEY = "systemName"
  val DEFAULT_SCHEMA_KEY = "defaultSchema"
  val DATABASE_NAME_KEY = "databaseName"

  val URL_KEY = "url"

  val USER_KEY = "user"
  val PASSWORD_KEY = "password"
  val DATABASE_TABLE_KEY = "dbtable"

  val driver = "com.ibm.as400.access.AS400JDBCDriver"
  val source = "com.chinasofti.ark.bdadp.datasources.jdbc.as400"

  var databaseName = ""
  var url = ""

  var user = ""
  var password = ""
  var dbtable = ""

  override def call(): StringData = {
    Builder.build("")
  }

  override def configure(componentProps: ComponentProps): Unit = {
    val systemName = componentProps.getString(SYSTEM_NAME_KEY, "localhost")
    val defaultSchema = componentProps.getString(DEFAULT_SCHEMA_KEY, "")
    val databaseName = componentProps.getString(DATABASE_NAME_KEY)

    user = componentProps.getString(USER_KEY)
    password = componentProps.getString(PASSWORD_KEY)
    dbtable = componentProps.getString(DATABASE_TABLE_KEY)

    url = s"jdbc:as400://$systemName/$defaultSchema;database name=$databaseName;naming=sql;prompt=false;errors=full;translate binary=true"

    debug("url: " + url)
  }

  override def spark(sparkScenarioOptions: SparkScenarioOptions): SparkData = {
    Builder.build(
      sparkScenarioOptions.sqlContext().read
        .format(source)
        .option(DRIVER_KEY, driver)
        .option(URL_KEY, url)
        .option(USER_KEY, user)
        .option(PASSWORD_KEY, password)
        .option(DATABASE_TABLE_KEY, dbtable)
        .load()
    )
  }
}
