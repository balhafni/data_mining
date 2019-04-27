import java.io.{FileWriter, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Bashar_Alhafni_ItemBasedCF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("itemBased_CF").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

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

    //training data
    val train_data = sc.textFile(training_file)

    val start = System.currentTimeMillis()
    //training data header
    val train_data_header = train_data.first()

    //take the header out
    val train_data_no_header = train_data.filter(data => data != train_data_header)

    //splitting data by comma
    val splitted_data = train_data_no_header.map(line => line.split(","))

    //saving the data as ((user,item),rating)
    val user_business_rating = splitted_data.collect().map(data => ((data(0), data(1)), data(2).toDouble))

    //converting the ((user,item),rating) rdd to a map
    val user_business_rating_map = user_business_rating.toMap

    //println("total training data "+user_item_rating_map.size)
    //get all the businesses that were rated by a particular user
    //(user,business1) (user,business2) .......
    val user_to_businesses_init = user_business_rating.map(data => (data._1._1, data._1._2))
    //parallelize the rdd
    val user_to_businesses_init_parallel = sc.parallelize(user_to_businesses_init).groupByKey()
    //group the businesses by user and convert it to a map --> (user,(business1,business2,...))
    val user_to_businesses = user_to_businesses_init_parallel.collectAsMap()

    //println("(user,(business1,business2....) size : "+user_to_businesses.size)
    //get all the users who rated a particular business
    //(business,user1) (business,user2) .......
    val business_to_users_init = user_business_rating.map(data => (data._1._2, data._1._1))
    //parellelize the rdd
    val business_to_users_init_parallel = sc.parallelize(business_to_users_init).groupByKey()
    //group the users by business and convert it to a map --> (business, (user1,user2,....))
    val business_to_users = business_to_users_init_parallel.collectAsMap()
    //println("(business,(user1,user2....) size : "+business_to_users.size)



    //getting the average rating for each user
    def getAvgUserRating(user: String, businesses: Iterable[String]):Double={
     var avgRating = 0.0
      businesses.map(b =>{
        avgRating = avgRating + user_business_rating_map(user,b)
      })

    return avgRating / businesses.size
    }

    val avgUserRatingMap = user_to_businesses_init_parallel.map(data =>{
      var user = data._1
      var businesses = data._2
      var avgRating = getAvgUserRating(user,businesses)
      (user, avgRating)
    }).collectAsMap()

    //getting the avg rating for each business
    def getAvgBusinessRating(business: String, users: Iterable[String]):Double={
      var avgRating = 0.0
      users.map(u =>{
        avgRating = avgRating + user_business_rating_map(u,business)
      })

      return avgRating / users.size
    }

    val avgBusinessRatingMap = business_to_users_init_parallel.map(data =>{
      var business = data._1
      var users = data._2
      var avgRating = getAvgBusinessRating(business,users)
      (business, avgRating)
    }).collectAsMap()


    //pearson correlations map
    //key; (business1, business2)
    //value: correlation
    val pearsonCorrelations = new scala.collection.mutable.HashMap[(String,String), Double]

    def getPearsonCorrelation(business1: String, business2:String): Double ={

      //if the pearson correlation of these two businesses already exist, return it
      if(pearsonCorrelations.contains((business1,business2))){
          return pearsonCorrelations(business1,business2)
      }

      if(pearsonCorrelations.contains((business2,business1))){
        return pearsonCorrelations(business2,business1)
      }

      //get all the users who rated business1


      val users_rated_business1 = business_to_users(business1).toSet
      //get all the users who rated business2
      val users_rated_business2 = business_to_users(business2).toSet
      //get all the users who rated both
      val users_rated_both = users_rated_business1.intersect(users_rated_business2)

      if(users_rated_both.size == 0){
        //if no corrated items
        //no corrated items!!
        return 0.0
      }
      var avg_rating_business1 = 0.0
      var avg_rating_business2 = 0.0

      //getting the avg ratings of both business1 and business2
      users_rated_both.map(user=>{
        avg_rating_business1 = avg_rating_business1 + user_business_rating_map((user,business1))
        avg_rating_business2 = avg_rating_business2 + user_business_rating_map((user,business2))
      })

      avg_rating_business1 = avg_rating_business1 / users_rated_both.size
      avg_rating_business2 = avg_rating_business2 / users_rated_both.size



      var numerator = 0.0

      var denominator_pt1 = 0.0
      var denominator_pt2 = 0.0
      var denominator = 0.0

      //getting the numerator of the formula:
      // Sum over all users who rated both businesses ((rating of business1 - avg rating of business1)*(rating of business2 - avg rating of business2))
      //denominator_pt1: Sum over all users who rated both businesses((rating of business1 - avg rating of business 1)^2)
      //denominator_pt2: Sum over all users who rated both businesses((rating of business2 - avg rating of business 2)^2)
      users_rated_both.map(user=>{

        numerator = numerator + ((user_business_rating_map((user,business1)) - avg_rating_business1) * (user_business_rating_map((user,business2)) - avg_rating_business2))
//        if(user_business_rating_map((user,business1)) - avg_rating_business1 == 0) {
////          println("USER ONLY RATED ONE ITEM")
//          denominator_pt1 = denominator_pt1 + Math.pow(user_business_rating_map((user, business1)), 2)
////          println("FIXED VALUE IS "+denominator_pt1)
//        }
//        else{
          denominator_pt1 = denominator_pt1 + Math.pow(user_business_rating_map((user,business1)) - avg_rating_business1, 2)
//        }
//        if(user_business_rating_map((user,business2)) - avg_rating_business2 == 0){
//          println("USER ONLY RATED ONE ITEM")
          //denominator_pt2 = denominator_pt2 + Math.pow(user_business_rating_map((user,business2)), 2)
//          println("FIXED VALUE IS "+denominator_pt2)
//          println("USER ONLY RATED ONE ITEM")
//        }
//      else{
        denominator_pt2 = denominator_pt2 + Math.pow(user_business_rating_map((user,business2)) - avg_rating_business2, 2)
//      }
      })

//     if(denominator_pt1 == 0.0)
//        println("PART 1 IS ZERO")
//      if(denominator_pt2 == 0)
//        println("PART 2 IS ZEROOOOO")
      //denominator: sqrt(denominator_pt1) * sqrt(denominator_pt2)
      denominator = Math.sqrt(denominator_pt1) * Math.sqrt(denominator_pt2)

      var correlation = 0.0
      if(denominator == 0.0 || numerator == 0.0){

      //  println("SIMILARITY IS ZERO!!!!! "+"numeraotr "+numerator +" denomonator "+denominator)
        return 0.0
      }

      else{
        correlation = numerator / denominator
        pearsonCorrelations.put((business1, business2),correlation)
        return correlation
      }
    }

    //let's get the predictions for the testing data
    //reading the testing data
    val testing_data = sc.textFile(testing_file)

    //reading the testing_data header
    val testing_data_header = testing_data.first()

    //testing data without header
    val test_data_without_header = testing_data.filter(data => data != testing_data_header)

    //split the line of each testing data by comma
    val splitted_testing_data = test_data_without_header.map(line => line.split(","))

    //read the testing data as ((user,business),rating)
    val user_business_rating_testing = splitted_testing_data.collect().map(data => ((data(0),data(1)),data(2).toDouble))

    //convert the above rdd to a map
    val user_business_rating_testing_map = user_business_rating_testing.toMap

    val predictions = user_business_rating_testing_map.map(data => {
      val test_user = data._1._1
      val test_business = data._1._2
      val actualRating = data._2

      //if the test user and the test business are present in training
      if (user_to_businesses.contains(test_user) && business_to_users.contains(test_business)) {
        //get all the businesses rated by the user in testing
        val businesses_rated_by_testUser = user_to_businesses(test_user)
        //find the correlations between the business in testing
        //and the other businesses that where rated by the user in training
        val neighborhoods = businesses_rated_by_testUser.map(train_business => {
          val pearsonCorr = getPearsonCorrelation(test_business, train_business)

          ((test_business, train_business), pearsonCorr)

        }).toList.sortBy(data => data._2).reverse.take(10)

        //find the prediction
        var numerator = 0.0
        var denominator = 0.0
        var prediction = 0.0
        //numerator: sum over all neighborhoods(rating of user testing on business in training * correlation)
        //denominator: sum over all neighborhoods (abs(correlation))

        neighborhoods.map(x => {

          numerator = numerator + (user_business_rating_map(test_user, x._1._2) * x._2)
          denominator = denominator + Math.abs(x._2)
          //            }
        })
        if (denominator != 0 && numerator != 0.0) {
          //            println("PREDICTION IS NOTTTTT 0!!!!!!!!!!!!!!!!!!!!!!!!!!")
          prediction = numerator / denominator
        } else {

          prediction = avgUserRatingMap(test_user)
        }
        ((test_user, test_business), (actualRating, prediction))

      }
      //if user in testing is in training, but not the business
      else if (user_to_businesses.contains(test_user) && !business_to_users.contains(test_business)) {
        //use the avg user rating as a prediction
        val prediction = avgUserRatingMap(test_user)
        ((test_user, test_business), (actualRating, prediction))

      }
      //if the user in testing is not in training, but the business is
      else if (!user_to_businesses.contains(test_user) && business_to_users.contains(test_business)) {
        //use the avg business rating as a business
        val prediction = avgBusinessRatingMap(test_business)
        ((test_user, test_business), (actualRating, prediction))
      } else {
        //if both the user and the business are not in training, use 3.0
        ((test_user, test_business), (actualRating, 3.0))
      }
    })

    val parallel_predictions = sc.parallelize(predictions.toSeq)


    val finalPredictionSort = parallel_predictions.sortBy(x=>(x._1._1,x._1._2))

    val range1 = finalPredictionSort.filter(data => (math.abs(data._2._1 - data._2._2) >= 0.toDouble && math.abs(data._2._1 - data._2._2) < 1.toDouble)).count()
    val range2 = finalPredictionSort.filter(data => (math.abs(data._2._1 - data._2._2) >= 1.toDouble && math.abs(data._2._1 - data._2._2) < 2.toDouble)).count()
    val range3 = finalPredictionSort.filter(data => (math.abs(data._2._1 - data._2._2) >= 2.toDouble && math.abs(data._2._1 - data._2._2) < 3.toDouble)).count()
    val range4 = finalPredictionSort.filter(data => (math.abs(data._2._1 - data._2._2) >= 3.toDouble && math.abs(data._2._1 - data._2._2) < 4.toDouble)).count()
    val range5 = finalPredictionSort.filter(data => (math.abs(data._2._1 - data._2._2) >= 4.toDouble)).count()


    println(">=0 and <1: " + range1)
    println(">=1 and <2: " + range2)
    println(">=2 and <3: " + range3)
    println(">=3 and <4: " + range4)
    println(">=4: " + range5)

    val MSE = finalPredictionSort.map { case ((user, business), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    val RMSE = math.sqrt(MSE)
    println("RMSE: " + RMSE)
    println("TIME: "+ (System.currentTimeMillis() - start)/1000 +" sec")
    var print = new PrintWriter(new FileWriter("Bashar_Alhafni_ItemBasedCF.txt"))

    finalPredictionSort.collect().foreach(x => {
      val user = x._1._1
      val business = x._1._2
      val rating =  x._2._2
      print.println(user+","+business+","+rating)
    })
    print.close()


  }
}
