import java.io.{FileWriter, PrintWriter}


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_replace, sum}

object Task2 {

  def main(args: Array[String]): Unit = {
    var scalaConfig = new SparkConf().setAppName("firsAssignmnet").setMaster("local[2]")
    val sparkContext = new SparkContext(scalaConfig)

    secondTask(scalaConfig, sparkContext)

  }
  def secondTask(scalaConfig:SparkConf, sparkContext: SparkContext): Unit ={

    val sqlContext = SparkSession.builder().appName("secondTask").config("spark.master","local").getOrCreate()
    val df = sqlContext.read.format("csv").option("header","true").load("/Users/alhafni/Desktop/stack-overflow-2018-developer-survey/survey_results_public.csv")

    //partitioning the data frame into two partitions
    val oldPartition = df.repartition(2)
    val start1 = System.currentTimeMillis()

    //getting the num of salaries per country
    val countryCount = df.filter((regexp_replace(df("Salary"),",","") =!="0") and (df("Salary") =!= "NA")).groupBy("Country").count().sort("Country")

    //getting the total num of salaries for all countries
    val totalCount = countryCount.agg(sum("count"))


    val start2 = System.currentTimeMillis()

    //repartitioning the df based on country
    val newPartition = df.repartition(2,df("Country"))



    var out2 = new PrintWriter(new FileWriter("secondTask.csv"))
    out2.print("Standard")
    oldPartition.rdd.mapPartitions(i => Array(i.size).iterator, true).collect().foreach(i => out2.print(","+i))
    out2.println(",Elapsed time: "+(System.currentTimeMillis() - start1))
    out2.print("Partition")
    newPartition.rdd.mapPartitions(i => Array(i.size).iterator, true).collect().foreach(i => out2.print(","+i))
    out2.println(",Elapsed time: "+(System.currentTimeMillis() - start2))
    out2.close()


  }
}
