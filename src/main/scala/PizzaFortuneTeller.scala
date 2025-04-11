import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    val entriesCount = pizzaSalesData.count()
    print("Total number of entries: " + entriesCount)


    //    // Group by the "weather" column and count occurrences
    //    val groupedData = pizzaSalesData.groupBy("pizza_id").count().orderBy("pizza_id")
    //
    //    // Show the results
    //    groupedData.show()
    //
    //    // Stop the SparkSession
    //    spark.stop()
  }
}