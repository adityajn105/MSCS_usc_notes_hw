import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}
import java.io.PrintWriter

object task2 {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss = SparkSession
      .builder()
      .appName("task1")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val k = Integer.parseInt(args(0))
    val support = Integer.parseInt(args(1))
    val input_file = args(2)
    val output_path = args(3)

    val sc = ss.sparkContext

    val start_time = System.nanoTime()

    val textRdd = sc.textFile(input_file)
    val rdd = textRdd.filter(line => line != "user_id,business_id")
      .map(line => line.split(","))
      .map(x => ( x(0), immutable.Set(x(1)) ) )
      .reduceByKey((x, y) => x | y)
      .filter( x => x._2.size > k )
      .map(x => x._2)


    val count = rdd.count()

    val frequent_candidates = rdd.mapPartitions( x => getItemSet(x, count, support) )
      .distinct().collect().toList

    val frequent_itemsets = rdd.mapPartitions( x => countFrequentItemset(x, frequent_candidates) )
      .reduceByKey(_+_)
      .filter( x => x._2 >= support )
      .map( x => x._1 ).collect().toList

    val ans = "Candidates:\n"+getOutputInFormat(frequent_candidates)+"\n\n"+"Frequent Itemsets:\n"+getOutputInFormat(frequent_itemsets)

    new PrintWriter(output_path) { write(ans); close }
    println("Duration:"+( 1.0*(System.nanoTime()-start_time)/1000000000 ))
  }

  def getOutputInFormat( itemsets : List[ Set[String] ] ): String = {
    val length_dict = mutable.HashMap[Int, ListBuffer[List[String]]]()
    var mx = 0
    var temp = mutable.ListBuffer[List[String]]()
    for (item <- itemsets) {
      temp = length_dict.getOrElse(item.size, mutable.ListBuffer[List[String]]())
      temp.append(item.toList.sorted)
      length_dict(item.size) = temp
      if (item.size > mx) {mx = item.size}
    }

    implicit val lengthContentOrdering: Ordering[List[String]] = new Ordering[List[String]] {
      override def compare(a: List[String], b: List[String]): Int = {
        for ((x, y) <- a zip b) {
          val compare = x.compareTo(y)
          if (compare != 0) {
            return compare
          }
        }
        0
      }
    }

    var ans = ListBuffer[String]()

    for ( i <- 1 to mx ){
      length_dict(i) = length_dict(i).sorted( lengthContentOrdering )
      var temp = ListBuffer[String]()
      for (list <- length_dict(i) ){
        temp.append( "(" + list.map(x => "\'"+x+"\'").mkString(", ") + ")" )
      }
      ans.append(temp.mkString(","))
    }

    ans.mkString("\n\n")
  }

  def countFrequentItemset(partition: Iterator[Set[String]],
                           frequent_candidates: List[ Set[String] ]): Iterator[ Tuple2[Set[String], Int] ] = {
    val count = mutable.HashMap[ Set[String], Int ]()
    val baskets = partition.toList
    for (basket <- baskets){
      for ( itemset <- frequent_candidates ) {
        if ( itemset.subsetOf(basket) ) {
          count(itemset) = count.getOrElse(itemset, 0) + 1
        }
      }
    }

    val freq_count = mutable.Set[ Tuple2[ Set[String], Int ] ]()
    for ( itemset <- frequent_candidates ){
      freq_count.add( (itemset, count.getOrElse(itemset, 0)) )
    }
    freq_count.iterator
  }

  def getItemSet(partition: Iterator[Set[String]],
                 cnt: Long, support: Integer ): Iterator[ Set[String] ] = {
    val baskets = partition.toList
    var singletons = mutable.Set[String]()
    for (basket <- baskets) {
      singletons = singletons.union(basket)
    }
    val threshold = (1.0 * support * baskets.length) / cnt

    val candidates = mutable.Set[ Set[String] ]()
    for (c <- singletons) {
      candidates.add(Set(c))
    }

    val frequent_candidates = mutable.Set[Set[String]]()
    var cand_size = 1
    while (candidates.size > 0) {
      var count = mutable.HashMap[Set[String], Int]()
      for (basket <- baskets) {
        for (candidate <- candidates) {
          if (candidate.subsetOf(basket)) {
            count(candidate) = count.getOrElse(candidate, 0) + 1
          }
        }
      }

      val frequent_itemset = mutable.Set[Set[String]]()
      for (candidate <- candidates) {
        if (count.getOrElse(candidate, 0) >= threshold) {
          frequent_itemset.add(candidate)
          frequent_candidates.add(candidate)
        }
      }

      candidates.clear()
      cand_size = cand_size + 1
      for (freq_items <- frequent_itemset.toList.combinations(2)) {
        val new_cand = freq_items(0).union(freq_items(1))
        if (new_cand.size == cand_size) {
          candidates.add(new_cand)
        }
      }
    }
    frequent_candidates.iterator
  }
}
