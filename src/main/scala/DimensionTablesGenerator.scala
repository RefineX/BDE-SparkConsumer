import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Random
import com.github.javafaker.Faker

object DimensionTablesGenerator {
  val random = new Random()
  val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val faker = new Faker()

  def randomDate(start: LocalDate, end: LocalDate): String = {
    val startEpochDay = start.toEpochDay
    val endEpochDay = end.toEpochDay
    val randomEpochDay = startEpochDay + random.nextInt((endEpochDay - startEpochDay).toInt)
    LocalDate.ofEpochDay(randomEpochDay).format(dateFormatter)
  }

  def main(args: Array[String]): Unit = {

    // Account Dimension Table
    val accountHeader = Seq("account_id", "customer_id", "account_type", "account_open_date")
    val accountRows = (1 to 200).map { i =>
      Seq(i.toString, (random.nextInt(100) + 1).toString, if (random.nextBoolean()) "Savings" else "Checking", randomDate(LocalDate.of(2010, 1, 1), LocalDate.of(2020, 12, 31)))
    }
    writeCsv("DimensionTables/account_dimension.csv", accountHeader, accountRows)

    // Customer Dimension Table
    val customerHeader = Seq("customer_id", "customer_name", "customer_email")
    val customerRows = (1 to 100).map { i =>
      Seq(i.toString, faker.name().fullName(), faker.internet().emailAddress())
    }
    writeCsv("DimensionTables/customer_dimension.csv", customerHeader, customerRows)

    // Branch Dimension Table
    val branchHeader = Seq("branch_id", "branch_name", "branch_city")
    val branchRows = (1 to 20).map { i =>
      Seq(i.toString, s"${faker.address().city()} Branch", faker.address().city())
    }
    writeCsv("DimensionTables/branch_dimension.csv", branchHeader, branchRows)

    // Channel Dimension Table
    val channelHeader = Seq("channel_id", "channel_name")
    val channelRows = Seq(
      Seq("1", "ATM"),
      Seq("2", "Online"),
      Seq("3", "Mobile"),
      Seq("4", "In-Person")
    )
    writeCsv("DimensionTables/channel_dimension.csv", channelHeader, channelRows)
  }

  def writeCsv(fileName: String, header: Seq[String], rows: Seq[Seq[String]]): Unit = {
    val file = new File(fileName)
    val writer = new java.io.PrintWriter(file)
    try {
      writer.println(header.mkString(","))
      rows.foreach(row => writer.println(row.mkString(",")))
    } finally {
      writer.close()
    }
  }
}