import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods

import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}


object task1 {

  // spark-submit --driver-memory 4g --executor-memory 4g --class task1 hw2.jar $ASNLIB/publicdata/train_review.json task1.scala.res
  def hash_func_generator(n: Int, bins: Int): mutable.ListBuffer[Tuple4[Int, Int, Int, Int]] ={
      val p = 1000000007
      val hash_funcs = mutable.ListBuffer[Tuple4[Int, Int, Int, Int]]()

      val r = scala.util.Random
      val upper = math.pow(2,20).toInt
      for ( i <- 0 to n ) {
        val a = r.nextInt(upper)
        val b = r.nextInt(upper)
        hash_funcs.append( (a,b,p,bins))
      }
      hash_funcs
  }

  def get_minhash_signature( hash_func:mutable.ListBuffer[Tuple4[Int, Int, Int, Int]],
                             values: Set[Int]): ListBuffer[Int] ={
    val signature = mutable.ListBuffer[Int]()
    for (  (a,b,p,m) <- hash_func  ){
        var hash_v = Integer.MAX_VALUE
        for (v <- values){
          hash_v = Integer.min( hash_v,  ((((a*v)+b)%p)%m).toInt  )
        }
        signature.append(hash_v)
    }
    signature
  }

  def generate_band_hash( pair: Tuple2[Int, ListBuffer[Int]],
                          b:Int, r:Int ): ListBuffer[ Tuple2[Tuple2[Int, Int], mutable.Set[Int] ]] = {
      val buss_id = pair._1
      val signature = pair._2.toList
      val ans = ListBuffer[Tuple2[Tuple2[Int, Int], mutable.Set[Int] ]]()
      for ( i <- 0 to b){
        val key = (i, signature(i))  //(r*b:r*(1+b) )
        val value = mutable.Set[Int]()
        value.add(buss_id)
        ans.append( ( key, value) )
      }
      ans
  }

  def jaccard( x1: Int, x2: Int, business_user_map: scala.collection.Map[Int, Set[Int]] ): Double ={
    val set1 = business_user_map(x1)
    val set2 = business_user_map(x2)
    (1.0*set1.intersect(set2).size) / set1.union(set2).size
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss = SparkSession
      .builder()
      .appName("task1")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val input_file = args(0)
    val output_path = args(1)

    val sc = ss.sparkContext

    val textRdd = sc.textFile(input_file)
    val rdd = textRdd.map( x => JsonMethods.parse(x).values.asInstanceOf[Map[String, Any]] )
              .map( json => (json.getOrElse("business_id", null).asInstanceOf[String], json.getOrElse("user_id", null).asInstanceOf[String]) )
              .distinct()

    val unq_users = rdd.map(x => x._2).distinct().collect()
    val user2idx = mutable.HashMap[String, Int]()
    var i = 0
    for ( usr <- unq_users ){ user2idx(usr) = i; i = i+1 }

    val unq_buss = rdd.map(x => x._1).distinct().collect()
    val buss2idx = mutable.HashMap[String, Int]()
    val idx2buss = mutable.HashMap[Int, String]()
    i = 0
    for ( buss <- unq_buss ){
      buss2idx(buss) = i;
      idx2buss(i) = buss
      i = i+1 }
    val total_users = unq_users.length

    val rdd_business = rdd.map(x=> ( buss2idx(x._1), user2idx(x._2) ) )
      .groupByKey().mapValues(x => x.toSet)

    val hash_funcs = hash_func_generator(70, total_users-1)
    val candidates = rdd_business.mapValues(x=> get_minhash_signature(hash_funcs, x))
      .flatMap(x => generate_band_hash(x,70,1))
      .reduceByKey( (x,y) => x.union(y) )
      .filter(x => x._2.size > 1 )
      .map(x=> x._2)
      .flatMap( x => x.toList.combinations(2).toList )
      .map(x =>  if( x(0) < x(1) ) (x(0), x(1)) else (x(1),x(0)) )
      .distinct()

    val business_users_map = rdd_business.collectAsMap()
    val result = candidates.map( x=> ( x._1, x._2, jaccard(x._1, x._2, business_users_map) ))
      .filter( x => x._3 >= 0.05 )
      .map( x => ( idx2buss(x._1), idx2buss(x._2), x._3 ) )
      .map( x => if(x._1 < x._2) ( x._1, x._2, x._3 ) else ( x._2, x._1, x._3 ) )
      .map( x => "{"+"\"b1\": \""+x._1+"\", \"b2\": \""+x._2+"\", \"sim\":"+x._3.toString+"}")
      .collect()

    new PrintWriter(output_path) { write(result.mkString("\n")); close }
  }
}
