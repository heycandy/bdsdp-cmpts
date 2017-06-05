package com.chinasofti.ark.bdadp.component

import java.util.Properties

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.{SparkData, StringData}
import com.chinasofti.ark.bdadp.component.api.sink.{SinkComponent, SparkSinkAdapter}
import com.chinasofti.ark.bdadp.util.common.StringUtils
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
  var numPartitions: Int = 0
  var mode: String = null
  var properties = new Properties();

  override def apply(inputT: StringData): Unit = {
//    info(inputT.getRawData)
  }

  override def configure(componentProps: ComponentProps): Unit = {
    conUrl = componentProps.getString("conUrl")
    table = componentProps.getString("table")
    userName = componentProps.getString("userName")
    passWord = componentProps.getString("passWord")
    numPartitions = componentProps.getInt("numPartitions",8)
    mode = componentProps.getString("mode","error")

    StringUtils.assertIsBlank(conUrl,table,userName,passWord);

//    默认为SaveMode.ErrorIfExists模式，该模式下，如果数据库中已经存在该表，则会直接报异常，导致数据不能存入数据库.另外三种模式如下：
//    SaveMode.Append 如果表已经存在，则追加在该表中；若该表不存在，则会先创建表，再插入数据；
//    SaveMode.Overwrite 重写模式，其实质是先将已有的表及其数据全都删除，再重新创建该表，最后插入新的数据；
//    SaveMode.Ignore 若表不存在，则创建表，并存入数据；在表存在的情况下，直接跳过数据的存储，不会报错。

    properties.put("user", userName);
    properties.put("password", passWord);


  }

  override def apply(inputT: SparkData): Unit = {
    inputT.getRawData.repartition(numPartitions).write.mode(mode).jdbc(conUrl,table,properties)

  }
}
