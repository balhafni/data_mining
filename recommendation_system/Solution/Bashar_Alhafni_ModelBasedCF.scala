import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.Files

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

import scala.Boolean


object Bashar_Alhafni_ModelBasedCF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    var sparkConfig = new SparkConf().setAppName("ModelBasedCF").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConfig)


//    val training_file = "/Users/alhafni/Desktop/repos/inf553_data_mining/hw2/Data/train_review.csv"
//    val testing_file = "/Users/alhafni/Desktop/repos/inf553_data_mining/hw2/Data/test_review.csv"
    var training_file = ""
    var testing_file = ""
    try{
       training_file = args(0)
       testing_file = args(1)
    }catch{
      case outOfBound:ArrayIndexOutOfBoundsException => println("no arguments have been provided .. aborting")
        System.exit(0)
    }

    var path = new File("directory")
    if (!path.exists()) {
      path.mkdir()
    }

    //to handle the stackoverflow
    sparkContext.setCheckpointDir("directory")


    //reading the data into an rdd
    var trainingRDD = sparkContext.textFile(training_file)
    var testingRDD = sparkContext.textFile(testing_file)

    //the header
    val trainingHead = trainingRDD.first()
    val testingHead = testingRDD.first()

    //taking the header out
    trainingRDD = trainingRDD.filter(x => x != trainingHead)
    testingRDD = testingRDD.filter(x => x != testingHead)


    val trainingRDD1 = trainingRDD.map(line => {
      val clean = line.split(",")
      (clean(0), clean(1), clean(2))
    })

    val testingRDD1 = testingRDD.map(line => {
      val clean = line.split(",")
      (clean(0), clean(1), clean(2))
    })


    var user_ids_map = new scala.collection.mutable.HashMap[String, Int]
    var business_ids_map = new scala.collection.mutable.HashMap[String, Int]
    var user_ids_back_map = new scala.collection.mutable.HashMap[Int, String]
    var business_ids_back_map = new scala.collection.mutable.HashMap[Int, String]

    var count_usr = 1
    var count_business = 1


    trainingRDD1.collect().foreach(line => {
      //add the user id to the user_ids map
      if (!user_ids_map.contains(line._1)) {
        user_ids_map.put(line._1, count_usr)
        user_ids_back_map.put(count_usr, line._1)
        count_usr = count_usr + 1
      }
      //adding the business ids to the business ids map
      if (!business_ids_map.contains(line._2)) {
        business_ids_map.put(line._2, count_business)
        business_ids_back_map.put(count_business, line._2)
        count_business = count_business + 1
      }
    })


    var trainingMap = trainingRDD1.map(line => {
      Rating(user_ids_map(line._1), business_ids_map(line._2), line._3.toDouble)
    })


    testingRDD1.collect().foreach(line => {
      //add the user id to the user_ids map
      if (!user_ids_map.contains(line._1)) {
        user_ids_map.put(line._1, count_usr)
//        user_ids_back_map.put(count_usr, line._1)
        count_usr = count_usr + 1
      }

      //adding the business ids to the business ids map
      if (!business_ids_map.contains(line._2)) {
        business_ids_map.put(line._2, count_business)
//        business_ids_back_map.put(count_business, line._2)
        count_business = count_business + 1
      }
    })

    var testingMap = testingRDD1.map(line => {
      Rating(user_ids_map(line._1), business_ids_map(line._2), line._3.toDouble)
    })

    // Build the recommendation model using ALS
    val rank = 2
    val numIterations = 30
    val model = ALS.train(trainingMap, rank, numIterations, 0.28, 4, 1)


    // Evaluate the model on rating data
    val usersBusinesses = testingMap.map { case Rating(user, business, rate) =>
      (user, business)
    }
    val predictions =
      model.predict(usersBusinesses).map { case Rating(user, business, rate) =>
        ((user, business), rate)
      }

    val ratesAndPreds = testingMap.map { case Rating(user, business, rate) =>
      ((user, business), rate)
    }.join(predictions)


    val range1 = ratesAndPreds.filter(data => (math.abs(data._2._1 - data._2._2) >= 0.toDouble && math.abs(data._2._1 - data._2._2) < 1.toDouble)).count()
    val range2 = ratesAndPreds.filter(data => (math.abs(data._2._1 - data._2._2) >= 1.toDouble && math.abs(data._2._1 - data._2._2) < 2.toDouble)).count()
    val range3 = ratesAndPreds.filter(data => (math.abs(data._2._1 - data._2._2) >= 2.toDouble && math.abs(data._2._1 - data._2._2) < 3.toDouble)).count()
    val range4 = ratesAndPreds.filter(data => (math.abs(data._2._1 - data._2._2) >= 3.toDouble && math.abs(data._2._1 - data._2._2) < 4.toDouble)).count()
    val range5 = ratesAndPreds.filter(data => (math.abs(data._2._1 - data._2._2) >= 4.toDouble)).count()


    println(">=0 and <1: " + range1)
    println(">=1 and <2: " + range2)
    println(">=2 and <3: " + range3)
    println(">=3 and <4: " + range4)
    println(">=4: " + range5)

    val MSE = ratesAndPreds.map { case ((user, business), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    val RMSE = math.sqrt(MSE)
    println("RMSE = " + RMSE)


    //mapping the predictions back from hashcodes to strings
    val finalOutput = ratesAndPreds.map(x => (user_ids_back_map(x._1._1), business_ids_back_map(x._1._2), x._2._2))

    var write = new PrintWriter(new FileWriter("Bashar_Alhafni_ModelBasedCF.txt"))

    //writing to a file
    val sortedOuput = finalOutput.sortBy(x => (x._1, x._2))
    sortedOuput.collect().foreach(x => {

      write.println(x._1 + "," + x._2 + "," + x._3)
    })

    write.close()
    //deleting the file directory
    deleteDir(path)


  }

  def deleteDir(dir: File): Boolean = {
    if (dir.isDirectory()) {
      var children = dir.listFiles()
      for (x <- children) {
        var delete = deleteDir(x)
        if(!delete)
          return false
      }

    }
    return dir.delete()


  }
}
