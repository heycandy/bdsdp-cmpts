package com.chinasofti.ark.bdadp.component

<<<<<<< HEAD
import java.text.SimpleDateFormat
import java.util.{Date, Calendar}
=======
import java.util.Calendar
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4

import com.chinasofti.ark.bdadp.component.api.Configureable
import com.chinasofti.ark.bdadp.component.api.data.SparkData
import com.chinasofti.ark.bdadp.component.api.sink.SinkComponent
import org.apache.commons.lang.StringUtils
import org.slf4j.Logger

/**
<<<<<<< HEAD
 * Created by White on 2017/5/10.
 */
class Assert(id: String, name: String, log: Logger)
  extends SinkComponent[SparkData](id, name, log) with Configureable {
=======
  * Created by White on 2017/5/10.
  */
class Assert(id: String, name: String, log: Logger)
    extends SinkComponent[SparkData](id, name, log) with Configureable {
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4

  var conditionExpr: String = _
  var deadline: String = _
  var period: String = _
  var assertKey: String = _
  var assertValue: String = _

  var assertFlag: Boolean = false

  override def apply(inputT: SparkData): Unit = {

    val Array(hour, minute, second) = deadline.split(":")
    val assertTime = () => {
      val calendar = Calendar.getInstance()

      calendar.get(Calendar.HOUR_OF_DAY) <= hour.toInt &&
<<<<<<< HEAD
        calendar.get(Calendar.MINUTE) <= minute.toInt &&
        calendar.get(Calendar.SECOND) <= second.toInt
=======
      calendar.get(Calendar.MINUTE) <= minute.toInt &&
      calendar.get(Calendar.SECOND) <= second.toInt
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
    }

    do {

      assertFlag = inputT.getRawData.filter(conditionExpr).count() != 0

      if (assertFlag) {
        Thread.sleep(period.toInt * 60 * 1000)
      }

    } while (assertFlag && assertTime())


    if (!assertFlag && assertKey.startsWith("$")) {
<<<<<<< HEAD
      //      assertValue = inputT.getRawData.col(assertKey.tail).toString()
      assertValue = getNowDate()
=======
      assertValue = inputT.getRawData.first().getAs(assertKey.tail)
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
    }

    System.setProperty("scenario.assert.flag", assertFlag.toString)
    System.setProperty(assertKey, assertValue)

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
<<<<<<< HEAD
      if (!StringUtils.isNumeric(str)) {
        throw new IllegalArgumentException(
          "Incorrect format for 'deadline', For example: 20:00:00.")
      })

  }

  def getNowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateFormat.format(now)
    today
  }

=======
                     if (!StringUtils.isNumeric(str)) {
                       throw new IllegalArgumentException(
                         "Incorrect format for 'deadline', For example: 20:00:00.")
                     })

  }
>>>>>>> c5c6e652a6967989a1d0e5a8aa802015dea6fab4
}
