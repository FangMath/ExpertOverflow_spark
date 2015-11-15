package com.fangfang.expertoverflow

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.Predef._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ LogManager, Level }

import scala.collection.mutable
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import breeze.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import scala.math

object Main{
  def main(args: Array[String]) {
//////////////////////////////////////
  val inputDir = args(0)
  val outputDir = args(1)
    println(inputDir)
    println(outputDir)
    val minSplits = 4

    System.setProperty("spark.executor.memory", "5g")
    System.setProperty("spark.rdd.compress", "true")

    println("Spark starting.")

    val conf = new SparkConf().setAppName("Main")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.default.parallelism", "12")
        .set("spark.hadoop.validateOutputSpecs", "false")

    conf.registerKryoClasses(Array(classOf[Post], classOf[User], classOf[Vote]))

    val sc = new SparkContext(conf)
//////////////////////////////////////
  println("Load data")
  //LOAD DATA USING SPARK
  val jsonData = sc.textFile(Post.file.getAbsolutePath, minSplits)

  println(Post.file.getAbsolutePath)

  val objData = jsonData.flatMap(Post.parse)
  objData.cache

  //val posts = objData.keyBy(_.id)

  //  val jsonVoteData = sc.textFile(Vote.file.getAbsolutePath, minSplits)
  //  val voteData = jsonVoteData.flatMap(Vote.parse)
  //  voteData.cache

  val jsonUserData = sc.textFile(User.file.getAbsolutePath, minSplits)
  val userData = jsonUserData.flatMap(User.parse)
  userData.cache

  //val votes = voteData.groupBy(_.postId)

  //cogroup === outer join
  //join === inner join
  //left outer join

  //val joinedData = posts.leftOuterJoin(votes)

  var query: RDD[Post] = objData

  Console.println("Begin the main command:")


//************** answer tags *******************//
  do {
//******** join, (ownerUserId, score, tags)
          val questionTags = query.filter(_.postTypeId==1).keyBy(_.id)
          val answersJoin = query.filter(_.postTypeId==2).keyBy(_.parentId).join(questionTags)
          val ansUserTag = answersJoin.map(x => (x._2._1.ownerUserId, x._2._1.score, x._2._2.tags))

//*************************************************//
//******** find top 1000 tags, build vocabulary
//*************************************************//
	  val num_tags = 100
          val tags = query.flatMap(_.tags).countByValue
	  val toptags = tags.toSeq.sortBy(_._2 * -1).take(num_tags)
	  val vocabulary = mutable.HashMap.empty[String, Int]
	  var k = 0
	  toptags.foreach { term =>
		vocabulary.put(term._1, k)
		k = k+1 }

          //println("Tags: " + toptags.mkString(";"))
	  //println(vocabulary)

//*************************************************//
//******** countVectorize function *********// 
//*************************************************//
def countVectorize(score: Int, taglist: Array[String]): SparseVector[Double] = {
	var index: Array[Int] = Array()

		taglist.foreach{
			term => 
				index = index :+ vocabulary.getOrElse(term, -1)
		}
	index = index.filter( _ >= 0)
		val values: Array[Double] = Array.fill(index.length)(score) 
		new SparseVector(index, values, num_tags)
}


//*************************************************//
//******** countVectorize tag score, then reduce ********//
//*************************************************//
val UserScorefull  = ansUserTag.map(x => (x._1, countVectorize(x._2, x._3)))

val byKey = UserScorefull.map({case (id,vec) => id->vec})
val UserScore = byKey.reduceByKey(_ + _).filter(_._2.activeSize > 0)
val UserScore2 = UserScore.map(x => (x._1, Vectors.sparse(x._2.length, x._2.index, x._2.data)))


//***********************************************************//
//******** fit by exponential distribution, transform data ********//
//***********************************************************//
// *** calculate the mean of each feature
val summary: MultivariateStatisticalSummary = Statistics.colStats(UserScore2.map(_._2))
//println(summary.mean) // a dense vector containing the mean value for each column
//println(summary.numNonzeros) // number of nonzeros in each column

def transform(vector: SparseVector[Double], scalingVec: Vector): Vector = {
    require(vector.length == scalingVec.size,
      s"vector sizes do not match: Expected ${scalingVec.size} but found ${vector.size}")
        val values = vector.data.clone()
        val dim = values.length
        var i = 0
        while (i < dim) {
          values(i) = 1- math.exp(- math.max(values(i),0)/scalingVec(vector.index(i)))
          i += 1
        }
        Vectors.sparse(vector.length, vector.index, values)
}

val rdd = UserScore.map(_._2)
val transData = rdd.map(x => transform(x, summary.mean))


//***********************************************************//
//******** k-means clustering ********//
//***********************************************************//
val numClusters = 6
val numIterations = 50
val clusters = KMeans.train(transData, numClusters, numIterations)

val WSSSE = clusters.computeCost(transData)
println("Within Set Sum of Squared Errors = " + WSSSE)

val label = clusters.predict(transData)
label.countByValue.foreach(println)

//******** end *****************
} while (readCommand)




  Console.println("Exit")

  def readCommand: Boolean = {

    println("come in the readCommand module!")

    val command = Console.readLine

    println(command)

    if (command == "?") Console.println("t=Topics, !=Execute")

    if (command.isEmpty) false
    else {
      command match {
        case "=" => //print
        case "~" => clear
        case "@" => {
          println("Cache RDD")
          query.cache
        }
        case _ => "Unknown command."
      }
      true
    }
  }

  def setQuery(rdd: RDD[Post]) = {
    query = rdd
  }

  def clear = {
    //clear
    println("Clearing all filters.")
    setQuery(objData)
  }

  def printlnT(f: => String) = {
    val i = time(f)
    println(i._1 + "ms\n" + i._2)
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis
    val result = block // call-by-name
    val t1 = System.currentTimeMillis
    ((t1 - t0), result)
  }
}

}
