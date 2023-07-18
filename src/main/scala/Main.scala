import com.typesafe.config.{Config, ConfigFactory}
import com.zaxxer.hikari.HikariDataSource

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Random

class DorisDAO {
  val ds: HikariDataSource = new HikariDataSource()
  val config: Config = ConfigFactory.load().getConfig("doris")
  ds.setJdbcUrl(config.getString("url"))
  ds.setDriverClassName(config.getString("dataSourceClassName"))
  ds.setUsername(config.getString("user"))
  ds.setPassword(config.getString("password"))
  ds.addDataSourceProperty("rewriteBatchedStatements", "true")

  val conn: Connection = ds.getConnection

  def insert(sql: String): Unit = {
    conn.prepareStatement(sql).execute()
  }
}

class Event {
  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  var user_id: Int = 0
  var date: Date = new Date()
  var city: String = "Shanghai"
  var age: Int = 0
  var sex: Int = 1
  var last_visit_date: Date = new Date()
  var cost: Int = 0
  var max_dwell_time: Int = 0
  var min_dwell_time: Int = 0

  def generator(random: Random): Event = {
    this.user_id += 1
    this.date.setTime(random.between(946656000000L, 1577807999000L))
    this.city = BigInt(256, random).toString(36).slice(0,10)
    this.age = random.between(18, 99)
    this.sex = random.between(0, 1)
    this.last_visit_date.setTime(random.between(946656000000L, 1577807999000L))
    this.cost = random.between(0, 1)
    this.min_dwell_time = random.between(0, 60)
    this.max_dwell_time = random.between(min_dwell_time, min_dwell_time+3000)
    this
  }

  override def toString = s"""($user_id, "${sdf.format(date)}", "$city", $age, $sex, "${sdf.format(last_visit_date)}", $cost, $max_dwell_time, $min_dwell_time)"""
}

object Main {
  def main(args: Array[String]): Unit = {
    val event: Event = new Event()
    val random: Random = new Random()

    val dao = new DorisDAO()

    val total = 100000000
    val batch = 1000
    val threads = 10

    while(event.user_id < 100000000) {
      dao.insert(
        s"""INSERT INTO example_tbl VALUES """ +
        s"""${(for (_ <- 1 to batch) yield event.generator(random).toString).mkString(", ")}""")
    }
  }
}