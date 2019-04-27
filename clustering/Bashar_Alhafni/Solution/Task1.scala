

import java.io.{FileWriter, PrintWriter}

import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.{HashingTF, IDF}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL.WithDouble._


object Task1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("clustering").setMaster("local")
    val sc = new SparkContext(sparkConf)
    var input_file = ""
    var feature = ""
    var numOfClusters = 0
    var maxIterations = 0

    try{
      input_file = args(0)
      feature = args(1)
      numOfClusters = args(2).toInt
      maxIterations = args(3).toInt
    }catch{
      case outOfBound:ArrayIndexOutOfBoundsException => println("no arguments have been provided .. aborting")
        System.exit(0)
    }

    val data = sc.textFile(input_file)

    val clean_data = data.map(x => x.split(" ").toSeq)

    val seed = 20181031

    if(feature.equalsIgnoreCase("W"))
      kmeans_word_count(clean_data, numOfClusters, seed, maxIterations)
    else if(feature.equalsIgnoreCase("T"))
      kmeans_tfidf(clean_data, numOfClusters, seed, maxIterations)

  }

  //finding which documents got assigned to which centroids
  //by looping through all the centroids and finding the closest ones
  def predict(centroids: mutable.HashMap[Int, Vector], tf:Vector):Int={
    var nearestCentroid = -1
    var distance = Integer.MAX_VALUE.toDouble

    centroids.keys.foreach(centroidNumber =>{
      val vector_of_centroid = centroids.apply(centroidNumber)
      val current_distance = Vectors.sqdist(tf, vector_of_centroid)
      if(current_distance < distance){
        distance = current_distance
        nearestCentroid = centroidNumber
      }
    })
    return nearestCentroid
  }

  //assigning the vectors to their nearest centroids
  def assignVectors(centroids:mutable.HashMap[Int, Vector], tf:RDD[Vector]): RDD[(Int, (Vector,Int))] ={
    val assignedVectors = tf.map(vector =>{
      var min_distatnce = Double.MaxValue
      var centroidNum = -1
      centroids.keys.foreach(key=>{
        val centroid = centroids.apply(key)
        val distance = Vectors.sqdist(centroid, vector)
        if(distance < min_distatnce){
          min_distatnce = distance
          centroidNum = key
        }
      })
      (centroidNum, (vector,1))
    })

    return assignedVectors
  }


  def kmeans_word_count(clean_data:RDD[Seq[String]], numOfClusters:Int, seed:Int, maxIterations: Int):Unit={
    //getting word count of the data
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(clean_data)
    //    tf.collect().foreach(x => println(x.size))

    val tf_collection = tf.collect()

    //let's get the number of unique words in the data
    val setOfwords = clean_data.map(x => x.toSet).collect().toList
    val allSets: mutable.Set[String] = mutable.Set[String]()
    for (set <- setOfwords) {
      allSets ++= set
    }

    var centroids: mutable.HashMap[Int, Vector] = mutable.HashMap[Int, Vector]()

    //a random object with a seed
    val rand = Random
    rand.setSeed(seed)

    //getting random centroids
    for(i <- 0 until numOfClusters){
      centroids.put(i, tf_collection(rand.nextInt(1001)))
    }

    //kmeans
    for (i <- 0 until maxIterations) {
      println("iteration "+i)
      //let's assign vectors data to their nearest centroid
      //looping through the assigned vectors for every centroid and summing them up
      //and keeping track of the number of vectors for every centroid
      val assignedVectors = assignVectors(centroids, tf).reduceByKey((a, b) => {
        val arr1 = a._1.toArray
        val arr2 = b._1.toArray
        var iterations = 0
        if (arr1.size <= arr2.size)
          iterations = arr1.size
        else {
          iterations = arr2.size
        }
        val sum: ListBuffer[Double] = ListBuffer[Double]()
        for (i <- 0 until iterations) {
          sum += arr1(i) + arr2(i)
        }
        val sizes = a._2 + b._2
        val result: Vector = new DenseVector(sum.toArray)
        (result, sizes)
      })

      //getting the new centroid by dividing the sum vectors by the number of vectors
      val newCentroids = assignedVectors.map(x => {
        val centroidNumber = x._1
        val vector = x._2._1.toArray
        val numberOfVectors = x._2._2
        for (i <- 0 until vector.length) {
          vector(i) = vector(i) / numberOfVectors
        }
        val newCentroid = new DenseVector(vector)
        (centroidNumber, newCentroid)
      }).collect()
      var newCentroids_hashMap: mutable.HashMap[Int, Vector] = mutable.HashMap[Int, Vector]()
      newCentroids.map(x =>{
        newCentroids_hashMap.put(x._1, x._2)
      })
      centroids = newCentroids_hashMap
    }

    //finding which documents belong to which cluster
    val centroids_to_tf = tf.map(vector =>{
      val centroidNumber = predict(centroids, vector)
      (centroidNumber, vector)
    }).groupByKey().sortBy(x => x._1)

    //let's create an RDD of tf vectors to list of words(i.e. documents)
    val tf_to_documents = clean_data.map(document => {
      val tf = hashingTF.transform(document)
      (tf, document)
    })


    //let's find to which clusters these documents belong to
    val clusters_to_documents = tf_to_documents.map(x => {
      val tf = x._1
      val document = x._2
      (predict(centroids, tf), document)
    })


    //grouping the documents by a cluster number and getting all the words in these documents
    val clusters_to_documents_map_flattened = clusters_to_documents.groupByKey().map(x => (x._1, x._2.toList.flatten))

    //clusters_to_documents_map_flattened.take(10).foreach(println)
    //getting the top 10 frequent words in each cluster
    val top_10_frequent_words = clusters_to_documents_map_flattened.map(x => {
      val cluster = x._1
      val words = x._2
      val map_res: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()
      words.foreach(word => {
        if (map_res.contains(word)){
          map_res.put(word, map_res.apply(word) + 1)
        }
        else {
          map_res.put(word, 1)
        }
      })
      val top_ten_words = map_res.toList.sortBy(_._2).reverse.take(10)
      (cluster, top_ten_words)
    }
    ).collectAsMap()


    val errors = centroids_to_tf.map(cluster_info => {
      val cluster_number = cluster_info._1
      val cluster_centroid = centroids.apply(cluster_number)
      val vectors_in_cluster = cluster_info._2
      var error = 0.0
      vectors_in_cluster.foreach(vector => {
        error = error + Vectors.sqdist(vector, cluster_centroid)
      })
      (cluster_number, vectors_in_cluster.size, error)
    })

    var WSSSE = 0.0
    val all_errors = errors.collect().toList
    all_errors.foreach(x =>{
      val error = x._3
      WSSSE = WSSSE + error
    })

    implicit val formats = org.json4s.DefaultFormats

    val clusters_output = all_errors.map {

      case (clusterNum, size, error) =>
        ("id", clusterNum) ~
          ("size", size) ~
          ("error", error) ~
          ("terms" -> top_10_frequent_words.apply(clusterNum).map {
            case (words, counts) =>
              (words)
          })

    }
    val algorithm_output = ("algorithm", "K-means") ~ ("WSEEE", WSSSE) ~ ("Clusters" -> clusters_output)
    println(pretty(render(algorithm_output)))
    val writer = new PrintWriter(new FileWriter("Bashar_Alhafni_KMeans_small_W_5_20.json"))
    writer.println(pretty(render(algorithm_output)))
    writer.close()
  }

  def kmeans_tfidf(clean_data:RDD[Seq[String]], numOfClusters:Int, seed:Int, maxIterations: Int):Unit={

    //getting word count of the data
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(clean_data)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    val tfidf_collection = tfidf.collect()

    //let's get the number of unique words in the data
    val setOfwords = clean_data.map(x => x.toSet).collect().toList
    val allSets: mutable.Set[String] = mutable.Set[String]()
    for (set <- setOfwords) {
      allSets ++= set
    }

    var centroids: mutable.HashMap[Int, Vector] = mutable.HashMap[Int, Vector]()

    //a random object with a seed
    val rand = Random
    rand.setSeed(seed)

    //getting random centroids
    for(i <- 0 until numOfClusters){
      centroids.put(i, tfidf_collection(rand.nextInt(1001)))

    }

    //kmeans
    for (i <- 0 until maxIterations) {
      println("iteration "+i)
      //let's assign vectors data to their nearest centroid
      //looping through the assigned vectors for every centroid and summing them up
      //and keeping track of the number of vectors for every centroid
      val assignedVectors = assignVectors(centroids, tfidf).reduceByKey((a, b) => {
        val arr1 = a._1.toArray
        val arr2 = b._1.toArray
        var iterations = 0
        if (arr1.size <= arr2.size)
          iterations = arr1.size
        else {
          iterations = arr2.size
        }
        val sum: ListBuffer[Double] = ListBuffer[Double]()
        for (i <- 0 until iterations) {
          sum += arr1(i) + arr2(i)
        }
        val sizes = a._2 + b._2
        val result: Vector = new DenseVector(sum.toArray)
        (result, sizes)
      })

      //getting the new centroid by dividing the sum vectors by the number of vectors
      val newCentroids = assignedVectors.map(x => {
        val centroidNumber = x._1
        val vector = x._2._1.toArray
        val numberOfVectors = x._2._2
        for (i <- 0 until vector.length) {
          vector(i) = vector(i) / numberOfVectors
        }
        val newCentroid = new DenseVector(vector)
        (centroidNumber, newCentroid)
      }).collect()
      var newCentroids_hashMap: mutable.HashMap[Int, Vector] = mutable.HashMap[Int, Vector]()
      newCentroids.map(x =>{
        newCentroids_hashMap.put(x._1, x._2)
      })
      centroids = newCentroids_hashMap
    }

    //finding which documents belong to which cluster
    val centroids_to_tfidf = tfidf.map(vector =>{
      val centroidNumber = predict(centroids, vector)
      (centroidNumber, vector)
    }).groupByKey().sortBy(x => x._1)

    //let's create an RDD of tf vectors to list of words(i.e. documents)
    val tf_to_documents = clean_data.map(document => {
      val tfidf = idf.transform(hashingTF.transform(document))
      (tfidf, document)
    })

    //let's find to which clusters these documents belong to
    val clusters_to_documents = tf_to_documents.map(x => {
      val tfidf = x._1
      val document = x._2
      (predict(centroids, tfidf), document)
    })


    //grouping the documents by a cluster number and getting all the words in these documents
    val clusters_to_documents_map_flattened = clusters_to_documents.groupByKey().map(x => (x._1, x._2.toList.flatten))

    //clusters_to_documents_map_flattened.take(10).foreach(println)
    //getting the top 10 frequent words in each cluster
    val top_10_frequent_words = clusters_to_documents_map_flattened.map(x => {
      val cluster = x._1
      val words = x._2
      val map_res: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()
      words.foreach(word => {
        if (map_res.contains(word)){
          map_res.put(word, map_res.apply(word) + 1)
        }
        else {
          map_res.put(word, 1)
        }
      })
      val top_ten_words = map_res.toList.sortBy(_._2).reverse.take(10)
      (cluster, top_ten_words)
    }
    ).collectAsMap()

    val errors = centroids_to_tfidf.map(cluster_info => {
      val cluster_number = cluster_info._1
      val cluster_centroid = centroids.apply(cluster_number)
      val vectors_in_cluster = cluster_info._2
      var error = 0.0
      vectors_in_cluster.foreach(vector => {
        error = error + Vectors.sqdist(vector, cluster_centroid)
      })
      (cluster_number, vectors_in_cluster.size, error)
    })

    var WSSSE = 0.0
    val all_errors = errors.collect().toList
    all_errors.foreach(x =>{
      val error = x._3
      WSSSE = WSSSE + error
    })

    implicit val formats = org.json4s.DefaultFormats

    val clusters_output = all_errors.map {

      case (clusterNum, size, error) =>
        ("id", clusterNum) ~
          ("size", size) ~
          ("error", error) ~
          ("terms" -> top_10_frequent_words.apply(clusterNum).map {
            case (words, counts) =>
              (words)
          })

    }
    val algorithm_output = ("algorithm", "K-means") ~ ("WSEEE", WSSSE) ~ ("Clusters" -> clusters_output)
    println(pretty(render(algorithm_output)))

    val writer = new PrintWriter(new FileWriter("Bashar_Alhafni_KMeans_small_T_5_20.json"))
    writer.println(pretty(render(algorithm_output)))
    writer.close()

  }
}
