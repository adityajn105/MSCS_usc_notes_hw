import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods

import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}


object task3train {

  //spark-submit --driver-memory 4g --executor-memory 4g --class task3train hw3.jar $ASNLIB/publicdata/train_review.json task3user.scala.model user_based
  def preprocess_user_based(json_text: String): Tuple2[String, immutable.Set[String]] ={
    val json_dict = JsonMethods.parse(json_text).values.asInstanceOf[Map[String, Any]]
    val value = immutable.Set[String]( (json_dict.getOrElse("business_id", null).asInstanceOf[String]) )
    ( json_dict.getOrElse("user_id", null).asInstanceOf[String], value )
  }

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

  def have3CommonBusinesses(uid1: Int, uid2: Int, utility_matrix_map: collection.Map[Int, Set[Int]]): Boolean ={
    utility_matrix_map(uid1).intersect(utility_matrix_map(uid2)).size >= 3
  }

  def have3CommonUsers(bid1: Int, bid2: Int, utility_matrix_map: collection.Map[Int, Set[Int]]): Boolean ={
    utility_matrix_map(bid1).intersect(utility_matrix_map(bid2)).size >= 3
  }

  def get_user_buss_stars(txt: String, user2idx: mutable.HashMap[String, Int], 
                          buss2idx: mutable.HashMap[String, Int]): Tuple2[ Int, immutable.Set[Tuple2[Int, Double]] ] = {
    
    val json =  JsonMethods.parse(txt).values.asInstanceOf[Map[String, Any]]
    val value = immutable.Set[Tuple2[Int, Double]](  (buss2idx(json.getOrElse("business_id", null).asInstanceOf[String]), json.getOrElse("stars", null).asInstanceOf[Double]) )
    
    ( user2idx(json.getOrElse("user_id", null).asInstanceOf[String]), value)
  }

  def preprocess_item_based(json_text: String): Tuple2[String, immutable.Set[String]] ={
    val json_dict = JsonMethods.parse(json_text).values.asInstanceOf[Map[String, Any]]
    val value = immutable.Set[String]( (json_dict.getOrElse("user_id", null).asInstanceOf[String]) )
    ( json_dict.getOrElse("business_id", null).asInstanceOf[String], value )
  }


  def takeAvg(x: Set[(Int, Double)]): mutable.Set[(Int, Double)] = {
    val mp = mutable.HashMap[ Int, ListBuffer[Double] ]()
    for ( (k,v) <- x ){
      if (mp.contains(k)){
        mp(k).append(v)
      }else {
        val temp = mutable.ListBuffer[Double]()
        temp.append(v)
        mp(k) = temp
      }
    }
    val ans = mutable.Set[(Int, Double)]()
    for ( (k,v) <- mp ){
      ans.add( (k, (1.0*v.sum)/v.length ) )
    }
    ans
  }

  def pearsonCorrelationUserBased(uid1: Int, uid2: Int, user_business_stars: collection.Map[Int, mutable.Set[(Int, Double)]]): Double = {
    val commonBusiness = user_business_stars(uid1).map(y => y._1).intersect( user_business_stars(uid2).map(y => y._1) )
    val business_star1 = mutable.HashMap[Int, Double]()
    for ( (k,v) <- user_business_stars(uid1) ){
      if(commonBusiness.contains(k)){ business_star1(k) = v}
    }

    val business_star2 = mutable.HashMap[Int, Double]()
    for ( (k,v) <- user_business_stars(uid2) ){
      if(commonBusiness.contains(k)){ business_star2(k) = v}
    }

    val avg1 = (1.0*business_star1.values.sum) / business_star1.size
    val avg2 = (1.0*business_star2.values.sum) / business_star2.size

    for ( (k,v) <- business_star1 ){ business_star1(k) = v-avg1 }
    for ( (k,v) <- business_star2 ){ business_star2(k) = v-avg2 }

    var num = 0.0
    var den1 = 0.0
    var den2 = 0.0
    for ( b <- commonBusiness ){
      num =  (num + ( business_star1(b) * business_star2(b) ))
      den1 = (den1 + ( math.pow( business_star1(b), 2 )))
      den2 = (den2+ (math.pow(business_star2(b), 2)))
    }
    val den = math.sqrt(den1)*math.sqrt(den2)
    if (den==0){0.0}
    else{num/den}
  }

  def get_buss_user_stars(txt: String, user2idx: mutable.HashMap[String, Int], buss2idx: mutable.HashMap[String, Int]): Tuple2[ Int, immutable.Set[Tuple2[Int, Double]] ] = {

    val json =  JsonMethods.parse(txt).values.asInstanceOf[Map[String, Any]]
    val value = immutable.Set[Tuple2[Int, Double]](  (user2idx(json.getOrElse("user_id", null).asInstanceOf[String]), json.getOrElse("stars", null).asInstanceOf[Double]) )

    ( buss2idx(json.getOrElse("business_id", null).asInstanceOf[String]), value)
  }

  def pearsonCorrelationItemBased(bid1: Int, bid2: Int, business_user_stars: collection.Map[Int, mutable.Set[(Int, Double)]]): Double = {
    val commonUsers = business_user_stars(bid1).map(y => y._1).intersect( business_user_stars(bid2).map(y => y._1) )
    val user_star1 = mutable.HashMap[Int, Double]()
    for ( (k,v) <- business_user_stars(bid1) ){
      if(commonUsers.contains(k)){ user_star1(k) = v}
    }

    val user_star2 = mutable.HashMap[Int, Double]()
    for ( (k,v) <- business_user_stars(bid2) ){
      if(commonUsers.contains(k)){ user_star2(k) = v}
    }

    val avg1 = (1.0*user_star1.values.sum) / user_star1.size
    val avg2 = (1.0*user_star2.values.sum) / user_star2.size

    for ( (k,v) <- user_star1 ){ user_star1(k) = v-avg1 }
    for ( (k,v) <- user_star2 ){ user_star2(k) = v-avg2 }

    var num = 0.0
    var den1 = 0.0
    var den2 = 0.0
    for ( b <- commonUsers ){
      num =  (num + ( user_star1(b) * user_star2(b) ))
      den1 = (den1 + ( math.pow( user_star1(b), 2 )))
      den2 = (den2+ (math.pow(user_star2(b), 2)))
    }
    val den = math.sqrt(den1)*math.sqrt(den2)
    if (den==0){0.0}
    else{num/den}
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss = SparkSession
      .builder()
      .appName("task3train")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val input_file = args(0)
    val model_file = args(1)
    val cf_type = args(2)

    val sc = ss.sparkContext

    if (cf_type == "user_based"){
      val user_business = sc.textFile(input_file).map(preprocess_user_based)
        .reduceByKey( (x,y) => x.union(y))

      val unq_users = user_business.map(x=>x._1).distinct().collect()
      val user2idx = mutable.HashMap[String, Int]()
      val idx2user = mutable.HashMap[Int, String]()
      var i = 0
      for (usr <- unq_users){ user2idx(usr) = i; idx2user(i) = usr; i = i+1 }

      val unq_business = sc.textFile(input_file)
        .map( x =>  JsonMethods.parse(x).values.asInstanceOf[Map[String, Any]])
        .map( x=> x.getOrElse("business_id", null).asInstanceOf[String] )
        .distinct().collect()
      val buss2idx = mutable.HashMap[String, Int]()
      i = 0
      for (buss <- unq_business){ buss2idx(buss) = i; i = i+1 }


      val hash_funcs = hash_func_generator(70, buss2idx.size - 1)
      val candidates = user_business.map( x => ( user2idx(x._1), x._2.map( (y: String) => buss2idx(y) ).toSet ) )
        .mapValues( buss_ids => get_minhash_signature(hash_funcs, buss_ids) )
        .flatMap( x=> generate_band_hash(x, 70, 1))
        .reduceByKey( (x,y) => x.union(y) )
        .filter(x => x._2.size > 1)
        .map( x=> x._2 )
        .flatMap( x => x.toList.combinations(2).toList )
        .map( x => if ( x(0) < x(1)) (x(0), x(1)) else (x(1), x(0)) )
        .groupByKey()
        .mapValues( x=> x.toSet )
        .flatMap( x => x._2.map( y => ( x._1, y ) ) )

      val utility_matrix_map = user_business.map( x => ( user2idx(x._1), x._2.map( y => buss2idx(y) ) ) )
        .collectAsMap()

      val user_business_stars = sc.textFile(input_file)
        .map(x=> get_user_buss_stars(x, user2idx, buss2idx) )
        .reduceByKey( (x,y) => x.union(y) )
        .mapValues(x => takeAvg(x))
        .collectAsMap()

      val ans = candidates.map( x => (x._1, x._2, jaccard(x._1, x._2, utility_matrix_map)))
        .filter( x => x._3 >= 0.01 & have3CommonBusinesses(x._1, x._2, utility_matrix_map))
        .map( x => ( idx2user(x._1), idx2user(x._2), pearsonCorrelationUserBased(x._1, x._2, user_business_stars) ) )
        .filter(x => x._3 > 0)
        .map( x=> if (x._1 < x._2) ( x._1, x._2, x._3 ) else (x._2, x._1, x._3) )
        .distinct()
        .map( x => "{"+"\"u1\": \""+x._1+"\", \"u2\": \""+x._2+"\", \"sim\":"+x._3.toString+"}" )
        .collect()

      new PrintWriter(model_file) { write(ans.mkString("\n")); close }

    }else{
      val bussiness_user = sc.textFile(input_file).map(x=>preprocess_item_based(x))
        .reduceByKey( (x,y)=> x.union(y))

      val unq_business = bussiness_user.map( x=> x._1 ).distinct().collect()
      val buss2idx = mutable.HashMap[String, Int]()
      val idx2buss = mutable.HashMap[Int, String]()
      var i = 0
      for ( buss <- unq_business ){ buss2idx(buss) = i; idx2buss(i) = buss; i = i+1 }

      val unq_user = sc.textFile(input_file)
        .map( x =>  JsonMethods.parse(x).values.asInstanceOf[Map[String, Any]])
        .map( x=> x.getOrElse("user_id", null).asInstanceOf[String] )
        .distinct().collect()
      val user2idx = mutable.HashMap[String, Int]()
      i = 0
      for (usr <- unq_user){ user2idx(usr) = i; i = i+1}

      val utility_matrix_map = bussiness_user.map( x => ( buss2idx(x._1), x._2.map( y => user2idx(y) ) ) )
        .collectAsMap()

      val business_user_stars = sc.textFile(input_file)
        .map(x=> get_buss_user_stars(x, user2idx, buss2idx) )
        .reduceByKey( (x,y) => x.union(y) )
        .mapValues(x => takeAvg(x))
        .collectAsMap()

      val ans = sc.parallelize( (0 to buss2idx.size-1).toList )
        .flatMap( x => (0 until x).toList.map( y => (y, x) ) )
        .filter( x =>  have3CommonUsers(x._1, x._2, utility_matrix_map))
        .map( x => ( idx2buss(x._1), idx2buss(x._2), pearsonCorrelationItemBased(x._1, x._2, business_user_stars) ) )
        .filter(x => x._3 > 0)
        .map( x=> if (x._1 < x._2) ( x._1, x._2, x._3 ) else (x._2, x._1, x._3) )
        .distinct()
        .map( x => "{"+"\"b1\": \""+x._1+"\", \"b2\": \""+x._2+"\", \"sim\": "+x._3.toString+"}" )
        .collect()

      new PrintWriter(model_file) { write(ans.mkString("\n")); close }
    }
  }
}
