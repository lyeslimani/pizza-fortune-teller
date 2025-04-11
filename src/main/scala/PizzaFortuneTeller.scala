import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, countDistinct, date_format, desc, explode, format_number, row_number, split, sum, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PizzaFortuneTeller {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Pizza Sales Analysis").master("local[*]").getOrCreate()

    val filePath = getClass.getResource("/pizza_sales.csv").getPath
    val pizzaSalesData: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    println("Structure of the entries:")
    pizzaSalesData.printSchema()
    val entriesCount = pizzaSalesData.count()
    println("Total number of entries: " + entriesCount)

    val salesByMonth = pizzaSalesData
      .withColumn("Month", date_format(to_date(col("order_date"), "M/d/yy"), "MMMM"))
      .groupBy("Month")
      .agg(
        sum("total_price").alias("Total_Sales"),
        sum(col("quantity")).alias("Pizza_Count")
      )
      .orderBy("Month")

    val outputPathMonth = "output/sales_by_month.csv"
    salesByMonth.repartition(1).write.option("header", "true").mode("overwrite").csv(outputPathMonth)
    println(s"Les résultats des ventes par mois ont été exportés vers : $outputPathMonth")

    val salesByDayOfWeek = pizzaSalesData
      .withColumn("DayOfWeek", date_format(to_date(col("order_date"), "M/d/yy"), "EEEE"))
      .groupBy("DayOfWeek")
      .agg(
        sum("total_price").alias("Total_Sales"),
        sum(col("quantity")).alias("Pizza_Count")
      )
      .orderBy("DayOfWeek")

    val outputPathDayOfWeek = "output/sales_by_day_of_week.csv"
    salesByDayOfWeek.repartition(1).write.option("header", "true").mode("overwrite").csv(outputPathDayOfWeek)
    println(s"Les résultats des ventes par jour de la semaine ont été exportés vers : $outputPathDayOfWeek")

    val mostSoldPizza = pizzaSalesData
      .groupBy("pizza_name")
      .agg(
        sum(col("quantity")).alias("Sales_Count")
      )
      .orderBy(desc("Sales_Count"))
      .limit(1)

    println("Pizza la plus vendue :")
    mostSoldPizza.show(false)

    val sizeMostSold = pizzaSalesData
      .groupBy("pizza_size")
      .agg(
        sum(col("quantity")).alias("Sales_Count")
      )
      .orderBy(desc("Sales_Count"))

    sizeMostSold.show(1)

    val pizzaMostSoldByMonth = pizzaSalesData
      .withColumn("Month", date_format(to_date(col("order_date"), "M/d/yy"), "MMMM"))
      .groupBy("Month", "pizza_name")
      .agg(
        sum(col("quantity")).alias("Sales_Count")
      )
      .withColumn("Rank", row_number().over(
        Window.partitionBy("Month").orderBy(desc("Sales_Count"))
      ))
      .filter(col("Rank") === 1)
      .drop("Rank")

    pizzaMostSoldByMonth.show(false)

    val outputPathPizzaByMonth = "output/most_sold_pizza_by_month.csv"
    pizzaMostSoldByMonth.repartition(1).write.option("header", "true").mode("overwrite").csv(outputPathPizzaByMonth)
    println(s"Le classement des pizzas les plus vendues par mois a été exporté vers : $outputPathPizzaByMonth")

    val sizeMostSoldByMonth = pizzaSalesData
      .withColumn("Month", date_format(to_date(col("order_date"), "M/d/yy"), "MMMM"))
      .groupBy("Month", "pizza_size")
      .agg(
        sum(col("quantity")).alias("Sales_Count")
      )
      .withColumn("Rank", row_number().over(
        Window.partitionBy("Month").orderBy(desc("Sales_Count"))
      ))
      .filter(col("Rank") === 1)
      .drop("Rank")

    println("Taille de pizza la plus vendue par mois :")
    sizeMostSoldByMonth.show(false)

    val outputPathSizeByMonth = "output/most_sold_size_by_month.csv"
    sizeMostSoldByMonth.repartition(1).write.option("header", "true").mode("overwrite").csv(outputPathSizeByMonth)
    println(s"Le classement des tailles de pizzas les plus vendues par mois a été exporté vers : $outputPathSizeByMonth")

    print("PARTIE 2: ")
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

    newStructureDfFormatted.show(false)


  }
}