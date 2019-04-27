import java.io.{FileWriter, PrintWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, regexp_replace}
import org.apache.spark.{SparkConf, SparkContext}

object Task3 {

  def main(args: Array[String]): Unit = {
    var scalaConfig = new SparkConf().setAppName("firsAssignmnet").setMaster("local[2]")
    val sparkContext = new SparkContext(scalaConfig)
    if(args.length > 1){
      val input_file = args(0)
      val output_file = args(1)
      thirdTask(input_file,output_file,scalaConfig, sparkContext)
    }else{
      println("the input and/or output argument(s) are missing ... aborting")
    }


  }

  def thirdTask(input:String,output:String,scalaConfig:SparkConf, sparkContext: SparkContext): Unit ={

    val sqlContext = SparkSession.builder().appName("thirdTask").config("spark.master","local").getOrCreate()

    //reading the data into a dataframe
    val df = sqlContext.read.format("csv").option("header","true").load(input)


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

    //getting the min salary for each country
    val minSalaryPerCountry = annualSalaries.groupBy(annualSalaries("Country")).agg(min(annualSalaries("Annual Salary")).alias("Min Salary")).sort(annualSalaries("Country"))

    //getting the max salary for each country
    val maxSalaryPerCountry = annualSalaries.groupBy("Country").agg(max("Annual Salary").alias("Max Salary")).sort("Country")

    //getting average salaries per country
    val avgAnnualSalaries = annualSalaries.groupBy("Country").agg(avg("Annual Salary").alias("Avg Salary")).sort("Country")

    //joining all the tables together
    var finalRes = salaryCountPerCountry.join(minSalaryPerCountry, "Country")
    finalRes = finalRes.join(maxSalaryPerCountry,"Country")
    finalRes = finalRes.join(avgAnnualSalaries, "Country").sort("Country")


    //writing to a file
    var out = new PrintWriter(new FileWriter(output))

    finalRes.collect().foreach(element => out.println(element(0).toString().replaceAll(",","") +","+element(1).toString().toDouble.toInt+","+element(2).toString().toDouble.toInt+","+element(3).toString().toDouble.toInt+
      ","+BigDecimal(element(4).toString().toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble))


    out.close()

  }
}
