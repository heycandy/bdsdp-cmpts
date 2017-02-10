import java.sql.DriverManager
import java.sql.Connection

object Test {

  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://10.100.66.118:3306/ark_bdadp_new?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true"
    val username = "root"
    val password = "rootadmin"

    var connection:Connection = null

    try {

      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select * from scenario")
      while ( resultSet.next() ) {
        val scenario_id = resultSet.getString("scenario_id")
        val scenario_name = resultSet.getString("scenario_name")
        println("scenario_id, password = " + scenario_id + ", " + scenario_name)
      }
    } catch {
      case e => e.printStackTrace
      //case _: Throwable => println("ERROR")
    }
    connection.close()
  }

}
