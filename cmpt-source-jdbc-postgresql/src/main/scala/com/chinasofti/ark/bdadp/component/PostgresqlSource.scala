package com.chinasofti.ark.bdadp.component

import java.util.Properties

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, SparkSourceAdapter}
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger


/**
 * Created by water on 2017.7.27.
 */
class PostgresqlSource(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log) with Configureable with
  SparkSourceAdapter[SparkData] {

  var conUrl: String = null
  var table: String = null
  var userName: String = null
  var passWord: String = null
  var partitionColumn: String = null
  var lowerBound: Long = 0
  var upperBound: Long = 0
  var numPartitions: Int = 0
  var properties = new Properties();
  var driver: String = null

  override def call(): StringData = {
    Builder.build("")
  }

  override def configure(componentProps: ComponentProps): Unit = {
    conUrl = componentProps.getString("conUrl")
    table = componentProps.getString("table", "*")
    userName = componentProps.getString("userName")
    passWord = componentProps.getString("passWord")

    partitionColumn = componentProps.getString("partitionColumn")
    StringUtils.assertIsBlank(conUrl, table, userName, passWord);
    lowerBound = componentProps.getInt("lowerBound", 1)
    upperBound = componentProps.getInt("upperBound", 10000000)
    numPartitions = componentProps.getInt("numPartitions", 8)

    properties.put("user", userName);
    properties.put("password", passWord);
    properties.put("driver", "org.postgresql.Driver");

  }

  override def spark(sparkScenarioOptions: SparkScenarioOptions): SparkData = {

    if(StringUtils.isBlank(partitionColumn)){
      Builder.build(sparkScenarioOptions.sqlContext().read.jdbc(conUrl, table, properties))
    }else{
      Builder.build(sparkScenarioOptions.sqlContext().read.jdbc(conUrl, table, partitionColumn, lowerBound, upperBound, numPartitions, properties))
    }
  }
}
