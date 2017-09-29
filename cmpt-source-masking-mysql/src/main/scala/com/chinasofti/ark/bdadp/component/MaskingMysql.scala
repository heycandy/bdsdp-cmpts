package com.chinasofti.ark.bdadp.component

import java.util.Properties

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SparkSourceAdapter, SourceComponent}
import com.chinasofti.ark.bdadp.util.common.StringUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.slf4j.Logger

/**
 * Created by water on 2017.7.21.
 */
class MaskingMysql(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log) with Configureable with
  SparkSourceAdapter[SparkData] {

  var conUrl: String = null
  var table: String = null
  var userName: String = null
  var passWord: String = null
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
    StringUtils.assertIsBlank(conUrl, table, userName, passWord);
    properties.put("user", userName);
    properties.put("password", passWord);
    properties.put("driver", "com.mysql.jdbc.Driver");

  }

  override def spark(sparkScenarioOptions: SparkScenarioOptions): SparkData = {
//    val loginUser = sparkScenarioOptions.getSettings.get("current.login.user");
    val loginUser = System.getProperty("loginUser")
    val tempDF = sparkScenarioOptions.sqlContext().read.jdbc(conUrl, "masking", properties)

    val conditionExpr = "username = '" + loginUser + "' and tablename ='" + table + "'"
    info("conditionExpr =======>" + conditionExpr)
    val filterDF = tempDF.filter(conditionExpr)
    val strDF = filterDF.selectExpr("viewname".split(","): _*)
    ("====== filterDF is ======" :: filterDF.toString() ::
      Nil ++ filterDF.repartition(8).take(10)).foreach(row => info(row.toString()))

    val colArr = org.apache.commons.lang.StringUtils.strip(strDF.collect().map(row => {
      row.toString()
    }).apply(0), "[]").split(",")
    info("---colArr---------"+colArr.mkString)
    val originDF = sparkScenarioOptions.sqlContext().read.jdbc(conUrl, table, properties)
    Builder.build(originDF.selectExpr(colArr: _*))
  }

}
