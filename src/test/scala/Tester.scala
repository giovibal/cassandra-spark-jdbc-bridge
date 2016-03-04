import java.util.{TimeZone, Date, Calendar, GregorianCalendar}

/**
 * Created by Giovanni Baleani on 04/03/2016.
 */
object Tester {

  def main(args: Array[String]) {



    val d = new Date()
    println(s"Weekly round [ NOW]: ${d.getTime} :: ${d}")

    val cal3 = new GregorianCalendar()
    cal3.setTime(d)
//    cal3.setTimeZone(TimeZone.getTimeZone("UTC"))
    cal3.setTimeZone(TimeZone.getTimeZone("GMT+4"))
    cal3.setTimeZone(TimeZone.getTimeZone("GMT+1"))
    cal3.set(Calendar.HOUR_OF_DAY, 17)
    println(s"            [GMT+4]: ${cal3.getTimeInMillis} :: ${cal3.getTime}")


    val d2 = weekRound(d)
    println(s"Weekly round [CALC]: ${d2.getTime} :: ${d2}")


    val cal2 = new GregorianCalendar()
    cal2.setTimeZone(TimeZone.getTimeZone("UTC"))
    cal2.setFirstDayOfWeek(Calendar.MONDAY)
    cal2.set(Calendar.YEAR, 2016)
    cal2.set(Calendar.MONTH, Calendar.FEBRUARY)
    cal2.set(Calendar.DAY_OF_MONTH, 29)
//    cal2.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    cal2.set(Calendar.HOUR_OF_DAY, 0)
    cal2.set(Calendar.MINUTE, 0)
    cal2.set(Calendar.SECOND, 0)
    cal2.set(Calendar.MILLISECOND, 0)
    println(s"Weekly round [TEST]: ${cal2.getTimeInMillis} :: ${cal2.getTime}")



  }

  def weekRound(d: Date) : Date = {
    val cal = new GregorianCalendar()
    cal.setTimeZone(TimeZone.getTimeZone("UTC"))
    cal.setTime(d)
    cal.setFirstDayOfWeek(Calendar.MONDAY)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTime
  }
}
