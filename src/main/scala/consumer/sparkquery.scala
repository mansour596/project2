package example
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io._

object sparkquery {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("sparkle")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

   val col1 = new StructType()
            .add("order_id", IntegerType, true)
            .add("customer_id", IntegerType, true)
            .add("customer_name", StringType, true)
            .add("product_id", IntegerType, true)
            .add("product_name", StringType, true)
            .add("product_category", StringType, true)
            .add("payment_type", StringType, true)
            .add("qty", IntegerType, true)
            .add("price", FloatType, true)
            .add("datetime", StringType, true)
            .add("country", StringType, true)
            .add("city", StringType, true)
            .add("ecommerce_website_name", StringType, true)
            .add("payment_txn_id", IntegerType, true)
            .add("payment_txn_success", StringType, true)
            .add("failure_reason", StringType, true)

    val data = spark.read
            .format("csv")
            .schema(col1)
            .load("/user/maria_dev/vanquish/vanquishData.csv")
  
    data.createOrReplaceTempView("vanquish")

        println("")
        println("Make A Selection")
        println("1: Display total number of records")
        println("2: Total Sales By Product Name")
        println("3: Top Buyers By Number Of Units")
        println("4: Total Number of Customers By Country")
        println("5: Most Popular Payment Type")
        println("6: Total Number Of Sales By Country")
        println("7: Total Sales By Website")
        println("x: Exit Menu")
             Iterator.continually(scala.io.StdIn.readLine)
            .takeWhile(_ != "x")
            .foreach{
              case "1" => displayall()
              case "2" => totalSoldByProduct()
              case "3" => topBuyerCountries()
              case "4" => totalNumCxByCountry()
              case "5" => topPayment()
              case "6" => totalSaleByCountry()
              case "7" => totalSalesWithYear()
            }

    //Start of the Query 
    def displayall(): Unit = {
    println("Displaying Total Number of Orders: ")
    val result = spark.sql("""select count(*)
                                from vanquish""")
    //result.printSchema()
    result.show()}

    def totalSoldByProduct(): Unit = {
    println("Total Sales By Product Name")
    val result2 = spark.sql("""SELECT product_name, price, sum(qty) as total_unit_sold, ROUND((sum(qty)*price), 2) as revenue
                                FROM vanquish
                                GROUP BY product_name, price
""")
    //result2.printSchema()
    result2.show()}

    def topBuyerCountries(): Unit ={
    println("Top Buyers By Number Of Units")
    val result3 = spark.sql("""select customer_name, country, sum(qty) as total_units
                                from vanquish
                                group by customer_name, country
                                having sum(qty) > 20
                                order by total_units desc
                                limit 20""")
    //result3.printSchema()
    result3.show()}

    def totalNumCxByCountry(): Unit ={
    println("Total Number of Unique Customers By Country")
    val result4 = spark.sql("""select country, count(distinct(customer_id)) as total_users
                                from vanquish
                                group by country
                                order by total_users desc""")
    //result4.printSchema()
    result4.show()} 
    
    def topPayment(): Unit ={
    println("Most Popular Payment Type")
    val result5 = spark.sql("""select payment_type, count(payment_type) as frequency
                                from vanquish
                                group by payment_type
                                order by frequency desc""")
    //result5.printSchema()
    result5.show()}
    
    def totalSaleByCountry(): Unit ={
    println("Total Number Of Sales By Country")
    val result6 = spark.sql("""SELECT country, SUM(total_sales)  as total_sales
                                FROM (
                                select country, CAST(sum(qty)*price AS BIGINT) as total_sales
                                from vanquish
                                group by country, price
                                order by total_sales) as sales
                                GROUP BY country
                                ORDER BY total_sales desc""")
    //result6.printSchema()
    result6.show()}

    def totalSalesWithYear(): Unit ={
    println("Total Sales By Website")
    val result7 = spark.sql("""SELECT ecommerce_website_name, SUM(total_sales)  as total_sales
                                FROM (
                                    select ecommerce_website_name, CAST(sum(qty)*price AS BIGINT) as total_sales
                                    from vanquish
                                    group by ecommerce_website_name, price) as sales
                                GROUP BY ecommerce_website_name
                                ORDER BY total_sales desc""")
    //result7.printSchema()
    result7.show()}

  }
}