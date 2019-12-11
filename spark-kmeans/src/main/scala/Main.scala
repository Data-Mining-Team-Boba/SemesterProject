import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{SQLContext, SparkSession, Row}

import com.mongodb.spark._
import com.mongodb.spark.sql._
import com.mongodb.spark.config.ReadConfig

object Main {
  def main(args: Array[String]): Unit = {
    println("======== = = == = == = = = = = Hello, World!")

    val spark = SparkSession.builder()
      .appName("spark-kmeans")
      .master("local[*]")
      .getOrCreate()

    // Read the data from MongoDB to a DataFrame
    val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "chess_data", "collection" -> "games_collection"))
    val df = spark.read.mongo(readConfig)

    // Drop '_id' column
    val rowRDD = df.drop("_id").drop("game_fen_positions").rdd

    // Parse rdd
    val parsedRdd = rowRDD.map(row => {
      println("============== = = = = = =  == HELLO WORLDIE" + row)

      row
    })
//    val rdd = df.select("game_matrix_positions").map(ele: Row => ele)
    parsedRdd.first()
//    println(df.count())

    /*
    * Steps:
    *   1. Convert data set
    *       We want it to be only one column with a list of values for each feature
    *   2. Build the model
    *       Feature ideas:
    *         - Attacking/Defending counts
    *         - Ratio of pieces in the forward half to back half
    *         - Something with pawns?
    *         - Queen position?
    *         - Trading pieces of equal value (this would be a way to detect a 'counter attack')
    *         - Avg centipawn change of the possible next moves? (using all possible moves and calculate the average centipawn change)
    *   3. Save the model (can load it back)
    *   4. Calculate error based on labelled boards
    */
  }

}
