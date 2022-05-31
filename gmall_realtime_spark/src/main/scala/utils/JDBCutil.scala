package utils

import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import javax.sql.DataSource

object JDBCUtil {

  val dataSource: DataSource = init()

  def init(): DataSource = {
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://localhost:3306/spark2020?useUnicode=true&characterEncoding=UTF-8")
    properties.setProperty("username", "root")
    properties.setProperty("password", "root")
    properties.setProperty("maxActive", "50")

    DruidDataSourceFactory.createDataSource(properties)
  }

  def getConnection: Connection = {
    dataSource.getConnection
  }

  def executeUpdate(conn: Connection, sql: String, params: Array[Any]): Int = {
    var res: Int = 0
    var pstat: PreparedStatement = null
    try {
      conn.setAutoCommit(false)
      pstat = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstat.setObject(i + 1, params(i))
        }
      }
      res = pstat.executeUpdate()
      conn.commit()
      pstat.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    res
  }

  def isExist(conn: Connection, sql: String, params: Array[Any]): Boolean = {

    var flag = false
    var pstat: PreparedStatement = null

    try {
      pstat = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstat.setObject(i + 1, params(i))
        }
      }
      flag = pstat.executeQuery().next()
      pstat.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag

  }


  def main(args: Array[String]): Unit = {
    val conn = getConnection
    val pstat = conn.prepareStatement("select userid from black_list")

    val rs = pstat.executeQuery()
    while (rs.next()) {
      println(rs.getString(1))
    }

    rs.close()
    pstat.close()
    conn.close()
  }

}
