package com.chinasofti.ark.bdadp.component

import java.util.Properties

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, SparkSourceAdapter}
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.slf4j.Logger


/**
  * Created by Administrator on 2017.2.8.
  */
class JDBCOracleSource(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log) with Configureable with
    SparkSourceAdapter[SparkData] {

  var conUrl: String = null
  var table: String = null
  var userName: String = null
  var passWord: String = null
  var partitionColumn: String = null    // 根据该字段分区，需要为整形，比如id等
  var lowerBound: Long = 0   // 分区的下界
  var upperBound: Long = 0    // 分区的上界
  var numPartitions: Int = 0    // 分区的个数
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
    lowerBound = componentProps.getInt("lowerBound",1)
    upperBound = componentProps.getInt("upperBound",10000000)
    numPartitions = componentProps.getInt("numPartitions",8)

    properties.put("user", userName);
    properties.put("password", passWord);
    properties.put("driver","oracle.jdbc.driver.OracleDriver");

  }

  override def spark(sparkScenarioOptions: SparkScenarioOptions): SparkData = {
    Builder.build(sparkScenarioOptions.sqlContext().read.jdbc(conUrl,table,properties))
  }
}
