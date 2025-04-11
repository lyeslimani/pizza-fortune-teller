import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PizzaFortuneTeller {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Pizza Sales Analysis").master("local[*]")
      .getOrCreate()

    val filePath = getClass.getResource("/pizza_sales.csv").getPath
    val pizzaSalesData: DataFrame = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Showing Basic infos of the dataset
    print("Structure of the entries:")
    pizzaSalesData.printSchema()

    print("\nRead Csv \n: ")
    pizzaSalesData.show()

    val entriesCount = pizzaSalesData.count()
    print("Total number of entries: " + entriesCount)

    val missingValues = pizzaSalesData.select(
      pizzaSalesData.columns.map(c =>
        sum(when(col(c).isNull || col(c) === "", 1).otherwise(0)).alias(c)
      ): _*
    )

    print("\nMissing values \n: ")
    missingValues.show(false)

    val dfWithParsedDate = pizzaSalesData.withColumn("parsed_order_date", to_date(col("order_date"), "M/d/yy"))

    val outliers = dfWithParsedDate.filter(
      col("quantity") <= 0 ||
        col("unit_price") <= 0 ||
        col("total_price") <= 0
    )

    println("Valeurs aberrantes détectées :")
    outliers.show(false)

    pizzaSalesData.describe().show(false)

  }
}