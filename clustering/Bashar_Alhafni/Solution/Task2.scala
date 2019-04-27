

import java.io.{FileWriter, PrintWriter}

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{BisectingKMeans, GaussianMixture, KMeans}
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL.WithDouble._



case class ClusteringAlgo(algorithm: String, WSSE: Double, clusters :List[Cluster])
case class Cluster(id: Int, size: Int, error:Double, top_10_frequent_words: List[(String,Int)])

object Task2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("clustering").setMaster("local")
    val sc = new SparkContext(sparkConf)

    var input_file = ""
    var algorithm = ""
    var numOfClusters = 0
    var maxIterations = 0

    try{
      input_file = args(0)
      algorithm = args(1)
      numOfClusters = args(2).toInt
      maxIterations = args(3).toInt
    }catch{
      case outOfBound:ArrayIndexOutOfBoundsException => println("no arguments have been provided .. aborting")
        System.exit(0)
    }

    val data = sc.textFile(input_file)


    //splitting the lines and putting the words in an rdd
    val clean_data = data.map(x => x.split(" ").toSeq)

    val seed = 42

      if(algorithm.equalsIgnoreCase("K")){
        kmeans(clean_data, numOfClusters, maxIterations, seed)
      }else if (algorithm.equalsIgnoreCase("B")){
        bisectingKMeans(clean_data, numOfClusters, maxIterations, seed)
      }
  }


  def kmeans(data: RDD[Seq[String]], clustersNum: Int, iterations: Int, seed: Int): Unit = {
    //getting the tf-idf scores for the reviews

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(data)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)


    val clusters = new KMeans().setSeed(seed).setK(clustersNum).setMaxIterations(iterations).run(tfidf)

    //cluster number to tfidf vector RDD
    val centriods = clusters.clusterCenters
    val vectors_to_clusters = tfidf.map(vector => {
      (clusters.predict(vector), vector)
    })

    //grouping the tfidf vectors by cluster number
    val clusters_to_vectors = vectors_to_clusters.groupByKey()

    //let's create an RDD of tf-idf vectors to list of words(i.e. documents)
    val tfidf_to_documents = data.map(document => {
      val tfidf = idf.transform(hashingTF.transform(document))
      (tfidf, document)
    })

    //let's find to which clusters these documents belong to
    val clusters_to_documents = tfidf_to_documents.map(x => {
      val tfidf = x._1
      val document = x._2
      (clusters.predict(tfidf), document)
    })


    //grouping the documents by a cluster number and getting all the words in these documents
    val clusters_to_documents_map_flattened = clusters_to_documents.groupByKey().map(x => (x._1, x._2.toList.flatten))


    //getting the top 10 frequent words in each cluster
    val top_10_frequent_words = clusters_to_documents_map_flattened.map(x => {
      val cluster = x._1
      val words = x._2
      val map_res: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()
      words.foreach(word => {
        if (map_res.contains(word))
          map_res.put(word, map_res.apply(word) + 1)
        else
          map_res.put(word, 1)
      })
      val top_ten_words = map_res.toList.sortBy(_._2).reverse.take(10)
      (cluster, top_ten_words)
    }
    ).collectAsMap()


    //errors for each cluster and sizes and sort by cluster number
    val errors = clusters_to_vectors.map(cluster_info => {
      val cluster_number = cluster_info._1
      val cluster_centroid = centriods(cluster_number)
      val vectors_in_cluster = cluster_info._2
      var error = 0.0
      vectors_in_cluster.foreach(vector => {
        error = error + Vectors.sqdist(vector, cluster_centroid)
      })
      (cluster_number, vectors_in_cluster.size, error)
    }).sortBy(x => x._1)


    // Evaluate clustering by computing Within Set Sum of Squared Errors

    val WSSSE = clusters.computeCost(tfidf)

    implicit val formats = org.json4s.DefaultFormats
    val clusters_output = errors.collect().toList.map {

      case (clusterNum, size, error) =>
        ("id", clusterNum) ~
          ("size", size) ~
          ("error", error) ~
          ("terms" -> top_10_frequent_words.apply(clusterNum).map {
            case (words, counts) =>
              (words)
          })

    }
    val algorithm_output = ("algorithm", "K-Means") ~ ("WSEEE", WSSSE) ~ ("Clusters" -> clusters_output)
    println(pretty(render(algorithm_output)))
    val writer = new PrintWriter(new FileWriter("Bashar_Alhafni_Cluster_small_K_8_20.json"))
    writer.println(pretty(render(algorithm_output)))
    writer.close()
  }

  def bisectingKMeans(data: RDD[Seq[String]], clustersNum: Int, iterations: Int, seed: Int):Unit={
    //getting the tf-idf scores for the reviews
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(data)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)


    val clusters = new BisectingKMeans().setSeed(seed).setK(clustersNum).setMaxIterations(iterations).run(tfidf)

    //cluster number to tfidf vector RDD
    val centriods = clusters.clusterCenters
    val vectors_to_clusters = tfidf.map(vector => {
      (clusters.predict(vector), vector)
    })

    //grouping the tfidf vectors by cluster number
    val clusters_to_vectors = vectors_to_clusters.groupByKey()

    //let's create an RDD of tf-idf vectors to list of words(i.e. documents)
    val tfidf_to_documents = data.map(document => {
      val tfidf = idf.transform(hashingTF.transform(document))
      (tfidf, document)
    })

    //let's find to which clusters these documents belong to
    val clusters_to_documents = tfidf_to_documents.map(x => {
      val tfidf = x._1
      val document = x._2
      (clusters.predict(tfidf), document)
    })


    //grouping the documents by a cluster number and getting all the words in these documents
    val clusters_to_documents_map_flattened = clusters_to_documents.groupByKey().map(x => (x._1, x._2.toList.flatten))

    //getting the top 10 frequent words in each cluster
    val top_10_frequent_words = clusters_to_documents_map_flattened.map(x => {
      val cluster = x._1
      val words = x._2
      val map_res: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()
      words.foreach(word => {
        if (map_res.contains(word))
          map_res.put(word, map_res.apply(word) + 1)
        else
          map_res.put(word, 1)
      })
      val top_ten_words = map_res.toList.sortBy(_._2).reverse.take(10)
      (cluster, top_ten_words)
    }
    ).collectAsMap()

    //errors for each cluster and sizes and sort by cluster number
    val errors = clusters_to_vectors.map(cluster_info => {
      val cluster_number = cluster_info._1
      val cluster_centroid = centriods(cluster_number)
      val vectors_in_cluster = cluster_info._2
      var error = 0.0
      vectors_in_cluster.foreach(vector => {
        error = error + Vectors.sqdist(vector, cluster_centroid)
      })
      (cluster_number, vectors_in_cluster.size, error)
    }).sortBy(x => x._1)

    val WSSSE = clusters.computeCost(tfidf)

    implicit val formats = org.json4s.DefaultFormats
    val clusters_output = errors.collect().toList.map {

      case (clusterNum, size, error) =>
        ("id", clusterNum) ~
          ("size", size) ~
          ("error", error) ~
          ("terms" -> top_10_frequent_words.apply(clusterNum).map {
            case (words, counts) =>
              (words)
          })
    }
    val algorithm_output = ("algorithm", "Bisecting K-Means") ~ ("WSEEE", WSSSE) ~ ("Clusters" -> clusters_output)
    println(pretty(render(algorithm_output)))

    val writer = new PrintWriter(new FileWriter("Bashar_Alhafni_Cluster_small_B_8_20.json"))
    writer.println(pretty(render(algorithm_output)))
    writer.close()
  }
}