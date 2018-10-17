
import java.io.{FileWriter, PrintWriter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_replace, sum}
import org.apache.spark.{SparkConf, SparkContext}

object Task1 {

  def main(args: Array[String]): Unit = {
    var scalaConfig = new SparkConf().setAppName("firsAssignmnet").setMaster("local[2]")
    val sparkContext = new SparkContext(scalaConfig)
    if(args.length > 1){
      val input_file = args(0)
      val output_file = args(1)
      firstTask(input_file,output_file,scalaConfig, sparkContext)
    }else{
      println("the input and/or output argument(s) are missing ... aborting")
    }
//    val input_file = "/Users/alhafni/Desktop/stack-overflow-2018-developer-survey/survey_results_public.csv"
//    val output_file = "firstTask.csv"

  }

  def firstTask(input:String, output:String,scalaConfig:SparkConf, sparkContext: SparkContext): Unit ={

    val sqlContext = SparkSession.builder().appName("firstTask").config("spark.master","local").getOrCreate()

    //reading the data into a data frame

    val df = sqlContext.read.format("csv").option("header","true").load(input)

    //getting the num of salaries per country
    val countryCount = df.filter((regexp_replace(df("Salary"),",","") =!="0") and (df("Salary") =!= "NA")).groupBy("Country").count().sort("Country")

    //getting the total num of salaries for all countries
    val totalCount = countryCount.agg(sum("count"))

    //writing to a file
    var out = new PrintWriter(new FileWriter(output))

    totalCount.collect().foreach(element => out.println("Total,"+element(0)))
    countryCount.collect().foreach(element => out.println(element(0).toString().replaceAll(",","") + "," +element(1)))

    out.close()

  }
}
