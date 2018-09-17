import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io._

object TestScala {
  def main(args: Array[String]): Unit = {
    var scalaConfig = new SparkConf().setAppName("firsAssignmnet").setMaster("local[2]")
    val sparkContext = new SparkContext(scalaConfig)

    firstTaskDataFrame(scalaConfig, sparkContext)
    thirdTask(scalaConfig, sparkContext)
    //firstTask
    //thirdTask
  }



  def firstTaskDataFrame(scalaConfig:SparkConf, sparkContext: SparkContext): Unit ={
//    var scalaConfig = new SparkConf().setAppName("firstTask").setMaster("local[2]")
//    val sparkContext = new SparkContext(scalaConfig)
    val sqlContext = SparkSession.builder().appName("firstTask").config("spark.master","local").getOrCreate()
    val df = sqlContext.read.format("csv").option("header","true").load("C:\\Users\\Bashar Alhafni\\IdeaProjects\\DataMiningHw1\\stack-overflow-2018-developer-survey\\survey_results_public.csv")
    val countryCount = df.filter((regexp_replace(df("Salary"),",","")*1=!="0") and (df("Salary") =!= "NA")).groupBy("Country").count().sort("Country")
    //val countryCount = df.filter("Salary != \"NA\" AND Salary != 0").groupBy("Country").count().sort("Country")

    countryCount.show()
    val totalCount = countryCount.agg(sum("count"))

    countryCount.collect().foreach(element => println(element(0) + "-->"+element(1)))
    totalCount.collect().foreach(element => println("Total " + element(0)))

    //writing to a file
    var out = new PrintWriter(new FileWriter("firstTask.csv"))

    totalCount.collect().foreach(element => out.println("Total,"+element(0)))
    countryCount.collect().foreach(element => out.println(element(0).toString().replaceAll(",","") + "," +element(1)))
    out.close()

    //third task


  }
  def cleanSalary(salary:String): Double ={
    return salary.replaceAll(",","").toDouble
  }
  def thirdTask(scalaConfig:SparkConf, sparkContext: SparkContext): Unit ={
//    var scalaConfig = new SparkConf().setAppName("firsAssignmnet").setMaster("local[2]")
//    val sparkContext = new SparkContext(scalaConfig)
    val sqlContext = SparkSession.builder().appName("thirdTask").config("spark.master","local").getOrCreate()
    val df = sqlContext.read.format("csv").option("header","true").load("C:\\Users\\Bashar Alhafni\\IdeaProjects\\DataMiningHw1\\stack-overflow-2018-developer-survey\\survey_results_public.csv")


    //salary count for each country
    val filteredDf = df.filter((df("Salary")=!="0") and (df("Salary") =!= "NA"))

    val salaryCountPerCountry = filteredDf.groupBy("Country").count()

    //getting the annual salary from monthly salary for each country
    val annualSalaryFromMonthly = filteredDf.filter((filteredDf("SalaryType")==="Monthly")).select(filteredDf("Country"),(regexp_replace(filteredDf("Salary"),",","")*12).alias("Annual Salary")).sort("Country")

    //getting the annual salary from weekly salary for each country
    val annualSalaryFromWeekly = filteredDf.filter((filteredDf("SalaryType")==="Weekly")).select(filteredDf("Country"),(regexp_replace(filteredDf("Salary"),",","")*52).alias("Annual Salary")).sort("Country")

    //getting the annual salary from annual salary for each country
    val annualSalaryFromAnnual = filteredDf.filter((filteredDf("SalaryType")=!="Weekly") and (filteredDf("SalaryType")=!="Monthly")).select(filteredDf("Country"),(regexp_replace(filteredDf("Salary"),",","")*1).alias("Annual Salary")).sort("Country")

    val annualSalaries = annualSalaryFromAnnual.union(annualSalaryFromWeekly).union(annualSalaryFromMonthly).sort("Country")

    //annualSalaries.show()
//    //getting the min salary for each country
     val minSalaryPerCountry = annualSalaries.groupBy(annualSalaries("Country")).agg(min(annualSalaries("Annual Salary")).alias("Min Salary")).sort(annualSalaries("Country"))
//
//    //getting the max salary for each country
    val maxSalaryPerCountry = annualSalaries.groupBy("Country").agg(max("Annual Salary").alias("Max Salary")).sort("Country")
//
//
//    //getting average salaries per country
    val avgAnnualSalaries = annualSalaries.groupBy("Country").agg(avg("Annual Salary").alias("Avg Salary")).sort("Country")
//
//
//    //joining all the tables together
    var finalRes = salaryCountPerCountry.join(minSalaryPerCountry, "Country")
    finalRes = finalRes.join(maxSalaryPerCountry,"Country")
    finalRes = finalRes.join(avgAnnualSalaries, "Country").sort("Country")


    //finalRes = finalRes.select(finalRes("Country"), finalRes("count"),finalRes("Min Salary"),finalRes("Max Salary"), finalRes("Avg Salary")).sort("Country")

    //writing to a file
    var out2 = new PrintWriter(new FileWriter("thirdTask.csv"))
//
    finalRes.collect().foreach(element => out2.println(element(0).toString().replaceAll(",","") +","+element(1).toString().toDouble.toInt+","+element(2).toString().toDouble.toInt+","+element(3).toString().toDouble.toInt+
      ","+BigDecimal(element(4).toString())))


//        finalRes.collect().foreach(element => out2.println(element(0).toString().replaceAll(",","") +","+element(1).toString()+","+element(2).toString()+","+element(3).toString()+
//         ","+element(4).toString()))
//
//
    //finalRes.collect().foreach(element => out2.println(element(0).toString().replaceAll(",","") +","+element(1).toString()+","+element(2).toString()+","+element(3).toString()+","+element(4).toString()))
    out2.close()

    finalRes.show()
   //minSalaryPerCountry.show()
  }


  def firstTask: Unit ={
    var scalaConfig = new SparkConf().setAppName("CountCountries").setMaster("local[2]")

    var scalaContext = new SparkContext(scalaConfig)

    val t0 = System.currentTimeMillis()
    val input_text = scalaContext.textFile("C:\\Users\\Bashar Alhafni\\IdeaProjects\\DataMiningHw1\\stack-overflow-2018-developer-survey\\survey_results_public.csv")

    //getting the header of the file
    val header = input_text.first()

    //removing the header of the file
    val input_withoutHeader = input_text.filter(line => line!=header)

    //printing every row on a line
    //println(input_withoutHeader.collect().mkString("\n"))

    //getting the column that has # from the file and splitting by ','
    // (so this will return the various countries)
    //in my example the column number is 1, in the hw it's 3
    val readrdd = input_withoutHeader.map(line => line.split(",")(3))
    //println(readrdd.collect().mkString("\n"))

    //for every word in readrdd, the mapper will output word,1
    //and the reducer will just sum the total
    var output = readrdd.map(word => (word,1)).reduceByKey(_+_).sortByKey(true,1)


    //printing the number of partitions
    println("The Number of partition is: "+output.getNumPartitions)
    //getting the total
    var total = output.map(_._2).sum()


    //writing the results to a csv file
    var print = new PrintWriter(new FileWriter("output.csv"))
    print.println("Total,"+total.toInt)

    output.collect().toList.foreach(tuple => print.println(tuple._1.replaceAll("\"","") +","+tuple._2))
    print.close()

    val tf = System.currentTimeMillis()

    println("The program took: "+(tf - t0) +"ms")
  }

}
