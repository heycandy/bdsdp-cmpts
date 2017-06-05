package com.chinasofti.ark.bdadp.component

import java.util.Calendar

import com.chinasofti.ark.bdadp.component.api.data.SparkData
import com.chinasofti.ark.bdadp.component.api.options.ScenarioOptions
import com.chinasofti.ark.bdadp.component.api.sink.SinkComponent
import com.chinasofti.ark.bdadp.component.api.{Configureable, Optional}
import org.apache.commons.lang.StringUtils
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

  var assertFlag: Boolean = false

  var options: ScenarioOptions = _

  override def apply(inputT: SparkData): Unit = {

    val Array(hour, minute, second) = deadline.split(":")
    val assertTime = () => {
      val calendar = Calendar.getInstance()

      calendar.get(Calendar.HOUR_OF_DAY) <= hour.toInt &&
      calendar.get(Calendar.MINUTE) <= minute.toInt &&
      calendar.get(Calendar.SECOND) <= second.toInt
    }

    do {

      assertFlag = inputT.getRawData.filter(conditionExpr).count() != 0

      if (assertFlag) {
        Thread.sleep(period.toInt * 60 * 1000)
      }

    } while (assertFlag && assertTime())


    if (!assertFlag) {
      if (assertValue.startsWith("$")) {
        options.getSettings.put(assertKey, inputT.getRawData.first().getAs(assertValue.tail))
      } else {
        options.getSettings.put(assertKey, assertValue)
      }

    }

    options.getSettings.put("scenario.assert.flag", String.valueOf(assertFlag))

  }

  override def configure(componentProps: ComponentProps): Unit = {
    conditionExpr = componentProps.getString("conditionExpr", "1=1")
    deadline = componentProps.getString("deadline", "05:00:00")
    period = componentProps.getString("period", "1")
    assertKey = componentProps.getString("assertKey", "scenario.assert.key")
    assertValue = componentProps.getString("assertValue", "scenario.assert.value")

    val splits = deadline.split(":")
    if (splits.length != 3) {
      throw new IllegalArgumentException("Incorrect format for 'deadline', For example: 20:00:00.")
    }

    splits.foreach(str =>
      if (!StringUtils.isNumeric(str)) {
        throw new IllegalArgumentException(
          "Incorrect format for 'deadline', For example: 20:00:00.")
      })

  }

  override def options(scenarioOptions: ScenarioOptions): Unit = {
    options = scenarioOptions
  }
}
