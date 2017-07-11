package com.chinasofti.ark.bdadp.component

import java.util.Calendar

import com.chinasofti.ark.bdadp.component.api.data.SparkData
import com.chinasofti.ark.bdadp.component.api.options.ScenarioOptions
import com.chinasofti.ark.bdadp.component.api.sink.SinkComponent
import com.chinasofti.ark.bdadp.component.api.{Configureable, Optional}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.DataFrame
import org.slf4j.Logger

/**
 * Created by White on 2017/5/10.
 */
class Assert(id: String, name: String, log: Logger)
    extends SinkComponent[SparkData](id, name, log) with Configureable with Optional {

  var conditionExpr: String = _
  var deadline: String = _
  var period: String = _
  var assertKey: String = _
  var assertValue: String = _

  var deadlineTimeMillis: Long = _
  var skip: Boolean = false
  var assertFlag: Boolean = false

  var options: ScenarioOptions = _

  override def apply(inputT: SparkData): Unit = {
    var df: DataFrame = null
    do {
      df = inputT.getRawData
      val totalCount = df.count()
      val filterCount = df.filter(conditionExpr).count()
      assertFlag = totalCount != 0 && totalCount == filterCount

      if (!assertFlag) {
        debug(String.format("sleep: %ss", String.valueOf(period.toInt * 60)))
        Thread.sleep(period.toInt * 60 * 1000)
      }

    } while (!assertFlag && System.currentTimeMillis() <= deadlineTimeMillis)


    if (assertFlag) {
      if (assertValue.startsWith("$")) {
        val value = df.first().getAs(assertValue.tail).toString
        info(String.format("setting: %s -> %s", assertKey, value))
        options.getSettings.put(assertKey, value)
      } else {
        info(String.format("setting: %s -> %s", assertKey, assertValue))
        options.getSettings.put(assertKey, assertValue)
      }

    } else if (skip) {
      info(String.format("setting: %s -> %s", "scenario.assert.flag", "false"))
      options.getSettings.put("scenario.assert.flag", "false")

    } else {
      throw new RuntimeException(getName)
    }

  }

  override def configure(componentProps: ComponentProps): Unit = {
    conditionExpr = componentProps.getString("conditionExpr", "1=1")
    deadline = componentProps.getString("deadline", "05:00:00")
    period = componentProps.getString("period", "1")
    assertKey = componentProps.getString("assertKey", "scenario.assert.key")
    assertValue = componentProps.getString("assertValue", "scenario.assert.value")
    skip = componentProps.getString("skip", "no").equals("yes")

    val splits = deadline.split(":")
    if (splits.length != 3) {
      throw new IllegalArgumentException("Incorrect format for 'deadline', For example: 20:00:00.")
    }

    splits.foreach(str =>
      if (!StringUtils.isNumeric(str)) {
        throw new IllegalArgumentException(
          "Incorrect format for 'deadline', For example: 20:00:00.")
      })

    val Array(hour, minute, second) = deadline.split(":")
    val calendar = Calendar.getInstance()

    calendar.set(Calendar.HOUR_OF_DAY, hour.toInt)
    calendar.set(Calendar.MINUTE, minute.toInt)
    calendar.set(Calendar.SECOND, second.toInt)

    deadlineTimeMillis = calendar.getTimeInMillis

  }

  override def options(scenarioOptions: ScenarioOptions): Unit = {
    options = scenarioOptions
  }
}
