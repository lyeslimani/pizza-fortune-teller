import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PizzaFortuneTeller {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Pizza Sales Analysis").master("local[*]")
      .getOrCreate()

    val filePath = "src/main/resources/pizza_sales.csv"
    val pizzaSalesData: DataFrame = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Showing Basic infos of the dataset
    print("Structure of the entries:")
    pizzaSalesData.printSchema()

    print("\nRead Csv \n: ")
    pizzaSalesData.show()

    val entriesCount = pizzaSalesData.count()
    print("Nombre total d'entrées : " + entriesCount)

    val missingValues = pizzaSalesData.select(
      pizzaSalesData.columns.map(c =>
        sum(when(col(c).isNull || col(c) === "", 1).otherwise(0)).alias(c)
      ): _*
    )

    print("\nValeurs manquantes : \n")
    missingValues.show(false)

    val dfWithParsedDate = pizzaSalesData.withColumn("parsed_order_date", to_date(col("order_date"), "M/d/yy"))

    val outliers = dfWithParsedDate.filter(
      col("quantity") <= 0 ||
        col("unit_price") <= 0 ||
        col("total_price") <= 0
    )

    println("Valeurs aberrantes détectées :")
    outliers.show(false)

    println("On utilise le .describe :")
    pizzaSalesData.describe().show(false)

    println("Restructuration du dataset :")

    val ingredientsExploded = dfWithParsedDate
      .withColumn("ingredient", explode(split(col("pizza_ingredients"), ",\\s*")))

    val ingredientsAgg = ingredientsExploded
      .groupBy("parsed_order_date")
      .pivot("ingredient")
      .sum("quantity")

    val newStructuredDf = dfWithParsedDate.groupBy("parsed_order_date")
      .agg(
        sum("total_price").alias("total_price_sum"),
        sum("quantity").alias("quantity_sum"),
        countDistinct("order_id").alias("order_count")
      )
      .join(
        dfWithParsedDate.groupBy("parsed_order_date").pivot("pizza_name").sum("quantity"),
        Seq("parsed_order_date"),
        "left"
      )
      .join(
        ingredientsAgg,
        Seq("parsed_order_date"),
        "left"
      )
      .join(
        dfWithParsedDate.groupBy("parsed_order_date").pivot("pizza_size").sum("quantity"),
        Seq("parsed_order_date"),
        "left"
      )
      .join(
        dfWithParsedDate.groupBy("parsed_order_date").pivot("pizza_category").sum("quantity"),
        Seq("parsed_order_date"),
        "left"
      )

    val newStructureDfFormatted = newStructuredDf.withColumn("total_price_sum", format_number(col("total_price_sum"), 2))
      .orderBy(col("parsed_order_date").asc)

    newStructureDfFormatted.show(31, false)

  }
}