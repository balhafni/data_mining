import java.io.{FileWriter, PrintWriter}


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_replace, sum}

object Task2 {

  def main(args: Array[String]): Unit = {
    var scalaConfig = new SparkConf().setAppName("firsAssignmnet").setMaster("local[2]")
    val sparkContext = new SparkContext(scalaConfig)

    if(args.length > 1){
      val input_file = args(0)
      val output_file = args(1)
      secondTask(input_file,output_file,scalaConfig, sparkContext)
    }else{
      println("the input and/or output argument(s) are missing ... aborting")
    }
    //    val input_file = "/Users/alhafni/Desktop/stack-overflow-2018-developer-survey/survey_results_public.csv"
    //    val output_file = "firstTask.csv"


  }
  def secondTask(input:String, output:String, scalaConfig:SparkConf, sparkContext: SparkContext): Unit ={

    val sqlContext = SparkSession.builder().appName("secondTask").config("spark.master","local").getOrCreate()
    val df = sqlContext.read.format("csv").option("header","true").load(input)

    //partitioning the data frame into two partitions
    val oldPartition = df.repartition(2)
    val start1 = System.currentTimeMillis()

    //getting the num of salaries per country
    val countryCount = oldPartition.filter((regexp_replace(df("Salary"),",","") =!="0") and (df("Salary") =!= "NA")).groupBy("Country").count().sort("Country")

    //getting the total num of salaries for all countries
    val totalCount = countryCount.agg(sum("count"))

    val end1 = System.currentTimeMillis()



    //repartitioning the df based on country
    val newPartition = df.repartition(2,df("Country"))

    val start2 = System.currentTimeMillis()

    //getting the num of salaries per country
    val countryCount2 = newPartition.filter((regexp_replace(df("Salary"),",","") =!="0") and (df("Salary") =!= "NA")).groupBy("Country").count().sort("Country")

    //getting the total num of salaries for all countries
    val totalCount2 = countryCount2.agg(sum("count"))

    val end2 = System.currentTimeMillis()


    var out2 = new PrintWriter(new FileWriter(output))
    out2.print("Standard")
    oldPartition.rdd.mapPartitions(i => Array(i.size).iterator, true).collect().foreach(i => out2.print(","+i))
    out2.println(","+(end1 - start1))
    out2.print("Partition")
    newPartition.rdd.mapPartitions(i => Array(i.size).iterator, true).collect().foreach(i => out2.print(","+i))
    out2.println(","+(end2- start2))
    out2.close()


  }
}
