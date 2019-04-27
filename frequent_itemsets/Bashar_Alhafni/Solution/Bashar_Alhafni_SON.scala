import java.io.{FileWriter, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Bashar_Alhafni_SON {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val start_time = System.currentTimeMillis()

    val sparkConf = new SparkConf().setAppName("freq_itemSets").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

//    val testing_file = "/Users/alhafni/Desktop/repos/inf553_data_mining/hw3/inf553_assignment3/Data/yelp_reviews_large.txt"

    var input_file =  ""
    var support = 0
    var output_file = ""
    try{
      input_file = args(0)
      support = args(1).toInt
      output_file = args(2)
    }catch{
      case outOfBound:ArrayIndexOutOfBoundsException => println("no arguments have been provided .. aborting")
        System.exit(0)
    }
    //reading the data
    val read_data = sc.textFile(input_file)

    //reading the data into
    //(review_index,word) pairs

    val reviews_words = read_data.map(x => {
      val splitted_line = x.split(",")
      val review_index = splitted_line(0)
      val word = splitted_line(1)
      (review_index, word)
    })


//    val support = 1000

    //grouping the pairs by review index
    val review_to_words = reviews_words.groupByKey()

    val total_count = review_to_words.collectAsMap().size


    //phase 1
    val first_pass_map = review_to_words.mapPartitions(x => apriori(x, support, total_count)).map(x => (x, 1))

    val first_pass_reduce = first_pass_map.reduceByKey( (a, b) => (1)).keys.collect()

//    println(first_pass_reduce.size)
    val second_pass_map = review_to_words.mapPartitions(x => filterCandidates(x, first_pass_reduce.toList))

//    second_pass_map.take(10).foreach(println)
    val second_reduce =  second_pass_map.reduceByKey(( a, b ) => (a+b))

    //let's filter out the non-frequent items
    val final_res = second_reduce.filter(x => x._2 >= support)

    //creating a set size,set mapping
    val prepare_output = final_res.map(x => (x._1.size, x._1.toList.sorted))

    //sorting by set
    val sorted_output = prepare_output.sortBy(x => x._2.mkString(","))

//    println("the total number of frequent items "+sorted_output.collect().size)

    //mapping the answer to (size of set, set)
    val output = sorted_output.groupByKey()

    val write_me = output.sortBy(x => x._1).collect()
//    println("the number of frequent items types "+write_me.size)
//    println("DETAILS: ")
//    write_me.foreach(x => println(x._1,x._2.size))
    val writer = new PrintWriter(new FileWriter(output_file))

    write_me.foreach(x => {
      val frequent_items = x._2
      frequent_items.foreach(item =>{
        writer.print("("+item.mkString(",")+")")
        if(frequent_items.toList(frequent_items.size - 1)!= item)
          writer.print(", ")
      })
      writer.println("")
    })

    writer.close()

    println("time taken: "+(System.currentTimeMillis() - start_time) / 1000 + " sec(s)")


  }

  //this function will take each partition in form of (basket, elements in that basket)
  //it will return an iterator of the frequent items
  def apriori(part: Iterator[(String, Iterable[String])], support:Int, total_count:Int): Iterator[Set[String]] = {

    val items_sets_to_buckets_sets = scala.collection.mutable.HashMap[Set[String], Set[String]]()
    val part_list = part.toList
    var size_partition = 0
    //for all the items in all the baskets
    part_list.foreach(x=>{
      //get the items in a particular basket
      val items_for_this_basket = x._2.toList
      //for every item in those items
      items_for_this_basket.foreach(item =>{
        //add this item to a set
        val single_item_set = Set[String](item)
        //add the basket number to a set
        val basket_for_this_item_set = Set[String](x._1)

        //add the set(items),set(baskets) to the map

        if(!items_sets_to_buckets_sets.contains(single_item_set)){
          items_sets_to_buckets_sets.put(single_item_set, basket_for_this_item_set)
        }else{
          val new_set = items_sets_to_buckets_sets.apply(single_item_set)++basket_for_this_item_set
          items_sets_to_buckets_sets.put(single_item_set, new_set)
        }
      })
      size_partition = size_partition + 1
    })

    //getting the singletons
    var res = mutable.ListBuffer[Set[String]]()
    var frequent_items =  scala.collection.mutable.HashMap[Set[String],Set[String]]()
    val threshhold = support * (part_list.size.toDouble/total_count.toDouble)

    items_sets_to_buckets_sets.foreach(x => {
      val item_set = x._1
      val baskets = x._2
      if(baskets.size >= threshhold) {
        res += item_set
        frequent_items.put(item_set, baskets)
      }
    })

//    println("the size of singletons is "+frequent_items.size)


    //creating a list of a list of sets. This will contain the list of frequent item sets
    //each frequent item category will be saved a list of sets, and to store all of them
    // we need to wrap these lists within a list
    val all_frequent_items_combos = mutable.ListBuffer[Set[String]]()

    //putting the singletons inside the list of lists

    all_frequent_items_combos ++= frequent_items.keySet

    var k = 2
    while(frequent_items.size != 0) {
      frequent_items = getFrequentItemsCombinations(frequent_items, threshhold, k)
      all_frequent_items_combos ++= frequent_items.keySet
//      println("the size of " + k + " tron " + frequent_items.size)
      k = k + 1
    }
    return all_frequent_items_combos.toIterator
  }

 /*
  This function will iterate through the candidate list twice and check if they occurrence is greater
  than the threshhold
  */
  def getFrequentItemsCombinations(freq_items:mutable.HashMap[Set[String], Set[String]], threshhold :Double, k:Int):mutable.HashMap[Set[String],Set[String]]={
    val res = mutable.HashMap[Set[String], Set[String]]()

    val keys_list = freq_items.keySet.toList
    for( i <- 0 until keys_list.size){
      for(j <- i + 1 until keys_list.size) {
        val item_set1 = keys_list(i)
        val item_set2 = keys_list(j)
        val baskets_set1 = freq_items(item_set1)
        val baskets_set2 = freq_items(item_set2)

        //let's union these two sets
        val new_items_set = item_set1.union(item_set2)
        if (new_items_set.size == k) {
          //let's find the common baskets the items occurred in
          val common_baskets = baskets_set1.intersect(baskets_set2)
          if (common_baskets.size >= threshhold) {
            res.put(new_items_set, common_baskets)

          }
        }
      }
    }
    return res
  }

  def filterCandidates(part: Iterator[(String, Iterable[String])], candidates: List[Set[String]]):Iterator[(Set[String], Int)]={
    val map_res = mutable.HashMap[Set[String],Int]()

    //converting the iterator to a list so we can iterate through it multiple times
    val part_list = part.toList
    //we want to check if the candidate is a subset of the items

    candidates.foreach(candidate =>{
      part_list.foreach(x => {
        val items = x._2.toSet

          //if the candidate is a subset, put it a map
          //the map contains (candidate, #baskets this item appeared in)
          if(candidate.subsetOf(items)){
            if(!map_res.contains(candidate)){
              map_res.put(candidate, 1)
            }else{
              map_res.put(candidate, map_res.apply(candidate) + 1)
            }

          }
        })
      })
    return map_res.toIterator
  }
}