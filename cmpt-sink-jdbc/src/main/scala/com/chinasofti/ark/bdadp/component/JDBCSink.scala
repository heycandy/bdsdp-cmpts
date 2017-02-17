package com.chinasofti.ark.bdadp.component

import java.util.Properties

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import org.slf4j.Logger

/**
 * Created by Administrator on 2017.2.16.
 */
class JDBCSink (id: String, name: String, log: Logger)
  extends SinkComponent[StringData](id, name, log) with Configureable with
  SparkSinkAdapter[SparkData] {

  var conUrl: String = null
  var table: String = null
  var userName: String = null
  var passWord: String = null
  var properties = new Properties();

  override def apply(inputT: StringData): Unit = {
//    info(inputT.getRawData)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    conUrl = componentProps.getString("conUrl")
    table = componentProps.getString("table")
    userName = componentProps.getString("userName")
    passWord = componentProps.getString("passWord")

    properties.put("user", userName);
    properties.put("password", passWord);
  }

  override def apply(inputT: SparkData): Unit = {
    inputT.getRawData.write.jdbc(conUrl,table,properties)

  }
}
