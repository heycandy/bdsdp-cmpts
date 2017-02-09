package com.chinasofti.ark.bdadp.component

import java.util.Properties

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{Builder, SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.options.SparkScenarioOptions
import com.chinasofti.ark.bdadp.component.api.source.{SourceComponent, SparkSourceAdapter}
import org.slf4j.Logger


/**
  * Created by Administrator on 2017.2.8.
  */
class JDBCSource(id: String, name: String, log: Logger)
  extends SourceComponent[StringData](id, name, log) with Configureable with
     SparkSourceAdapter[SparkData] {

     var conUrl: String = null
     var table: String = null
     var userName: String = null
     var passWord: String = null
     var properties = new Properties();

     override def call(): StringData = {
       Builder.build("")
     }

     override def configure(componentProps: ComponentProps): Unit = {
       conUrl = componentProps.getString("conUrl")
       table = componentProps.getString("table", "*")
       userName = componentProps.getString("userName")
       passWord = componentProps.getString("passWord")

       properties.put("user", userName);
       properties.put("password", passWord);
     }

     override def spark(sparkScenarioOptions: SparkScenarioOptions): SparkData = {
       Builder.build(sparkScenarioOptions.sqlContext().read.jdbc(conUrl,table,properties))
     }
   }
