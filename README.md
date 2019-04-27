# data_mining
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
  
  ```
  spark-submit --class <className> <JarFileName.jar> <train_dataset> <test_dataset>
  ```
  Where:<br/>
  \<className> should be subtituted by Bashar_Alhafni_ItemBasedCF or Bashar_Alhafni_ModelBasedCF <br />
  <JarFileName.jar> should be subtituted by hw2/Bashar_Alhafni/Solution/Bashar_Alhafni_hw2.jar <br />
  <train_dataset> is the absolute path of the training dataset (train_review.csv). This could be found in hw2/Data.zip <br />
  <test_dataset> is the absolute path of the testing dataset (test_review.csv). This could be found in hw2/Data.zip <br />
  
  So to run Item-based CF system, the following command should be executed:
  
   ```
   spark-submit --class Bashar_Alhafni_ItemBasedCF Bashar_Alhafni_hw2.jar train_review.csv test_review.csv
   ```

* #### hw3: Frequent Itemsets ####
  
  * SON Algorithm: The goal of this project was to implement the SON algorithm to identify frequent words in Yelp reviews. This was implemented on the small and very large datasets as part of the [Yelp Challenge](https://www.yelp.com/dataset/challenge).
  
   ##### Executing the code:
  
    To run the SON algorithm, the following command must be executed:
    ```
    spark-submit --class <className> <JarFileName.jar> <yelp_reviews_data> <support_threshhold> <output_file>
    ```
     Where:<br/>
    \<className> should be subtituted by Bashar_Alhafni_SON  <br />
    <JarFileName.jar> should be subtituted by hw3/Bashar_Alhafni/Solution/Bashar_Alhafni_SON.jar <br />
    <yelp_reviews_data> is the absolute path of the yelp reviews data. The small dataset could be found in hw3/Data.zip and large dataset can be found [here](https://drive.google.com/file/d/1Yi2iy5jV96Q8q6FAitw8WWeeZe8hT1Nj/view). <br/>
    <support_threshhold> is the support threshhold that the SON algorithm takes as an input<br />
     <output_file> is the absolute path for the output. <br />

     So to run the SON algorithm os the large dataset, the following command should be executed:
    ```
    spark-submit --class Bashar_Alhafni_SON Bashar_Alhafni_SON.jar yelp_reviews_large.txt 100000 output.txt
    ```

    NOTE: Since the dataset is huge, we might need to use the ```--driver-memory``` option to increase the memory that spark uses.

* #### hw4: Clustering ####
  * K-means: The goal of this project was to implement the mentioned algorithms on the yelp reviews dataset. The algorithm was implemented using word counts or tf-idf as features to compute the Euclidean Distances. 
   
   ##### Executing the code:
   
    ```
    spark-submit --class <className> <JarFileName.jar> <yelp_reviews_data> <feature_type> <num_of_clusters> <max_iterations>
    ```
    
    Where:<br/>
    \<className> should be subtituted by Task 1 or Task 2. Task 1 for word counts as features and Task 2 for tf-idf as features<br />
    <JarFileName.jar> should be subtituted by hw4/Bashar_Alhafni/Solution/Bashar_Alhafni_SON.jar <br />
    <yelp_reviews_data> is the absolute path of the yelp reviews data. The small dataset could be found in hw4/Data.zip<br/>
    <feature_type> is W for word counts or T for tf-idf <br />
    <num_of_clusters> is the number of clusters we would like to create<br />
    <max_iterations> max iteration until the termination of the algorithm <br />

    So to run the K-means algorithms using word counts as features to create 5 clusters in 20 iterations, the following command must be executed:
    
    ```
    spark-submit --class Task1 Bashar_Alhafni_Clustering.jar yelp_reviews_ clustering_small.txt W 5 20
    ```

   
* #### hw5: Streaming ####
  * Reservoir Sampling Algorithm: The goal of this project is to use the Twitter API of streaming to implement the fixed size sampling method (Reservoir Sampling Algorithm) and use the sampling to track the popular tags on tweets and calculate the average length of tweets.
  
   ##### Executing the code:
   
   ```
   spark-submit --class <className> <jarFileName>
   ```
   
   Where:<br/>
    \<className> should be subtituted by TwitterStreaming. <br />
    <JarFileName.jar> should be subtituted by hw5/Bashar_Alhafni/Solution/Bashar_Alhafni_hw5.jar <br />
   
   So to run the reservoir sampling algorithm, the following command must be executed:
   
   ```
   spark-submit --class TwitterStreaming ~alhafni/Desktop/repos/inf553_data_mining/hw5/Bashar_Alhafni/Solution/ Bashar_Alhafni_hw5.jar
   ```
  
