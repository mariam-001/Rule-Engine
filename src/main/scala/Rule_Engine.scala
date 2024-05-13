import java.sql.{Connection, DriverManager, PreparedStatement, Date}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import java.io.{File, PrintWriter}

object DiscountCalculator extends App {

  case class Order(timestamp: String, product_name: String, expire_date: String, quantity: Int, unit_price: Double, channel: String, payment_method: String)

  // Define paths for log files
  val logsFilePath = "C:\\Users\\dell\\Desktop\\Scala\\Scala Project\\logs.txt"
  val logWriter = new PrintWriter(new File(logsFilePath))

  // Function to write log messages
  def writeLog(message: String): Unit = {
    val timestamp = java.time.LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val formattedMessage = s"$timestamp - $message"
    println(formattedMessage) // Print log message to console
    logWriter.println(formattedMessage) // Write log message to log file
  }

  // Day Remaining Qualifier function
  def daysRemainingQualifier(timestamp: String, expireDate: String): Int = {
    // Get only the date part from timestamp column
    val timeStampDate = timestamp.take(10)

    // Convert string expiry date to LocalDate
    val expireDateTime = LocalDate.parse(expireDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val expireDateOnly = Date.valueOf(expireDateTime)

    // Convert string timestamp date to LocalDate
    val timeStampDateTime = LocalDate.parse(timeStampDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val timeStampDateOnly = Date.valueOf(timeStampDateTime)

    // Calculate remaining days until expiry
    val remainingDays = java.time.temporal.ChronoUnit.DAYS.between(timeStampDateOnly.toLocalDate(), expireDateOnly.toLocalDate()).toInt
    remainingDays
  }

  // Function to parse CSV line into Order
  def parseCsvLine(line: String): Order = {
    val fields = line.split(",")
    Order(
      fields(0),
      fields(1),
      fields(2),
      fields(3).toInt,
      fields(4).toDouble,
      fields(5),
      fields(6)
    )
  }

  // Function to read data from CSV file and parse it into List[Order]
  def readDataFromFile(filePath: String): List[Order] = {
    val logMessage = s"Reading data from file: $filePath"
    writeLog(logMessage)
    Source.fromFile(filePath)
      .getLines()
      .drop(1) // Skip header
      .map(parseCsvLine)
      .toList
  }

  // Function to calculate remaining days until expiry
  def remainingDaysUntilExpiry(order: Order): Int = {
    daysRemainingQualifier(order.timestamp, order.expire_date)
  }

  // Function to apply qualifying rules and calculate discounts
  def applyDiscounts(orders: List[Order]): Map[Order, Double] = {
    val logMessage = "Applying discounts to orders..."
    writeLog(logMessage)
    orders.map { order =>
      val remainingDays = remainingDaysUntilExpiry(order)
      val availableDiscounts = List(
        calculateDiscount(order),
        cheeseWineDiscount(order.product_name),
        specialMarch23Discount(order.timestamp),
        quantityDiscount(order.quantity),
        appDiscount(order.channel),
        visaCardDiscount(order.payment_method)
      )
      val qualifiedDiscounts = availableDiscounts.filter(_ > 0.0)
      val topTwoDiscounts = qualifiedDiscounts.sorted.takeRight(2)
      val averageDiscount = if (topTwoDiscounts.nonEmpty) topTwoDiscounts.sum / topTwoDiscounts.length else 0.0
      order -> averageDiscount
    }.toMap
  }

  // Function to calculate discount based on remaining days until expiry
  def calculateDiscount(order: Order): Double = {
    val remainingDays = remainingDaysUntilExpiry(order)
    if (remainingDays > 29) {
      0.0
    } else {
      (30 - remainingDays.toDouble) /100 // Discount rate is equal to the number of remaining days
    }
  }

  // Function to calculate discount for cheese and wine products
  def cheeseWineDiscount(productName: String): Double = {
    if (productName.toLowerCase.contains("cheese")) 0.10
    else if (productName.toLowerCase.contains("wine")) 0.05
    else 0.0
  }

  // Function to apply special discount for products sold on March 23rd
  def specialMarch23Discount(timestamp: String): Double = {
    if (timestamp.startsWith("2023-03-23")) 0.50
    else 0.0
  }

  // Function to calculate quantity discount
  def quantityDiscount(quantity: Int): Double = {
    quantity match {
      case q if q >= 6 && q <= 9 => 0.05
      case q if q >= 10 && q <= 14 => 0.07
      case q if q > 15 => 0.10
      case _ => 0.0
    }
  }

  // Function to apply discount based on sales channel (App)
  def appDiscount(channel: String): Double = {
    if (channel.equalsIgnoreCase("App")) 0.05
    else 0.0
  }

  // Function to apply discount for Visa card payments
  def visaCardDiscount(paymentMethod: String): Double = {
    if (paymentMethod.equalsIgnoreCase("Visa")) 0.05
    else 0.0
  }

  // Function to store results in an Oracle database table
  def storeResultsInDatabase(results: Map[Order, Double]): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    val url = "jdbc:oracle:thin:@//localhost:1521/XE"
    val username = "HR"
    val password = "123"

    try {
      // Open a connection
      connection = DriverManager.getConnection(url, username, password)
      val logMessage = "Connected to database..."
      writeLog(logMessage)

      // Prepare a statement
      val sql = "INSERT INTO orders (order_date, expiry_date, days_to_expiry, product_name, quantity, unit_price, channel, payment_method, discount, total_due) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      preparedStatement = connection.prepareStatement(sql)

      // Iterate over results and insert into database
      results.foreach { case (order, discount) =>
        preparedStatement.setDate(1, Date.valueOf(order.timestamp.take(10))) // Assuming timestamp is in ISO format
        preparedStatement.setDate(2, Date.valueOf(order.expire_date.take(10)))
        preparedStatement.setInt(3, remainingDaysUntilExpiry(order)) // Calculate days to expiry
        preparedStatement.setString(4, order.product_name)
        preparedStatement.setInt(5, order.quantity)
        preparedStatement.setDouble(6, order.unit_price)
        preparedStatement.setString(7, order.channel)
        preparedStatement.setString(8, order.payment_method)
        preparedStatement.setDouble(9, discount * 100)

        // Calculate total due after discount
        val totalDue = order.unit_price - (order.unit_price * discount )
        preparedStatement.setDouble(10, totalDue)

        preparedStatement.executeUpdate()
      }

      val successLogMessage = "Data successfully stored in database."
      writeLog(successLogMessage)
    } catch {
      case e: Exception =>
        val errorMessage = s"Error occurred while storing data in database: ${e.getMessage}"
        writeLog(errorMessage)
    } finally {
      // Close resources
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
      val closeConnectionLogMessage = "Closed database connection."
      writeLog(closeConnectionLogMessage)
    }
  }

  // Read data from CSV file
  val orders = readDataFromFile("C:\\Users\\dell\\Desktop\\Scala\\Scala Project\\TRX1000.csv")

  // Apply qualifying rules and calculate discounts
  val discounts = applyDiscounts(orders)

  // Store results in database
  storeResultsInDatabase(discounts)

  // Close log writer
  logWriter.close()
}
