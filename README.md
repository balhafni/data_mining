# inf553_data_mining
Note: This work was done using Spark 2.3.1 and Scala 2.11.0
* #### hw1: Analyzing the [Stackoverflow 2018 Developer Survey ](https://www.kaggle.com/stackoverflow/stack-overflow-2018-developer-survey) data ####
  * 1) Task 1: Computing the total number of survey responses per country that have provided a salary value.
  * 2) Task 2: Showing the number of partitions for the RDD built in Task 1 and show the number of items per partition as well as improving the performance of map and reduce tasks.
  * 3) Task 3: Computing the annual salary averages per country and show min and max salaries.
  
  ##### Executing the code:
  ```
  spark-submit --class <className> <JarFileName.jar> <input_file> <output_file>
  ```
  Where:<br/>
  \<className> should be subtituted by Task1, Task2, or Task3 to execute the tasks described above. <br />
  <JarFileName.jar> is should be subtituted by hw1/Bashar_Alhafni/Solution/Bashar_Alhafni.jar <br />
  <input_file> is the absolute path for the big Stackoverflow csv [data](https://www.kaggle.com/stackoverflow/stack-overflow-2018-developer-survey)<br />
  <output_file> is the absolute path for the output. <br />
  So to run Task 1, the following command should be executed:
  ```
  spark-submit --class Task1 Bashar_Alhafni.jar stack-overflow-2018-developer-survey/survey_results_public.csv output.csv
  ```
   
  
* #### hw2: Recommendation Systems as part of the [Yelp Challenge](https://www.yelp.com/dataset/challenge) ####
  
  * 1) Model-based Collaborative Filtering:
    This implementation of this part of the assignment was done by using Spark MLlib. I was able to accomplish a Root Mean Square Error (RMSE) of 1.076.

  * 2) Item-based Collaborative Filtering: After inspecting the data thoroughly, I was able to get to build an Item Based CF system by marely using the averages instead of complex pearson correlations. This is mainly because of the existence of many users in the dataset who rated one and only one business. 
 Using this simple method, I was able to obtain a RMSE of 1.08.

  ##### Executing the code:

   To run the Model-based CF System, the following command must be executed:

   ```
   spark-submit --class Bashar_Alhafni_ModelBasedCF Bashar_Alhafni_hw2.jar train_review.csv test_review.csv
   ```
 
   To run the Item-based CF system, the following command must be executed:

   ```
   spark-submit --class Bashar_Alhafni_ItemBasedCF Bashar_Alhafni_hw2.jar train_review.csv test_review.csv
   ```


#### hw3: Frequent Itemsets

#### hw4: Clustering

#### hw5: Streaming
