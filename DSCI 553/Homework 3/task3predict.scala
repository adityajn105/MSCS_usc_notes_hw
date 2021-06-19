import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods

import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}



object task3predict {

  // spark-submit --driver-memory 4g --executor-memory 4g --class task3predict hw2.jar $ASNLIB/publicdata/train_review.json $ASNLIB/publicdata/test_review.json task3user.scala.model task3user.scala.predict user_based
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

  def predict(x: (Int, mutable.Set[(Int, Double)]), N: Int, avg_dict: mutable.HashMap[Int, Double], model: collection.Map[(Int, Int), Double], unk_id: Int, cf_type: String): Tuple2[Int, Double] = {

    if (cf_type == "item_based") {
      val target_id = x._1
      val target_id_avg = avg_dict.getOrElse(target_id, avg_dict(unk_id))

      var result = ListBuffer[Tuple2[Double, Double]]()
      for ((id, score) <- x._2) {
        if (target_id < id) {
          result.append((score, model.getOrElse((target_id, id), 0.0)))
        }
        else {
          result.append((score, model.getOrElse((id, target_id), 0.0)))
        }
      }
      result = result.sortBy(x => -1 * x._2).slice(0, N)
      val num = result.map(x => x._1 * x._2).sum
      if (num == 0) { return (target_id, target_id_avg) }

      val den = result.map(x => x._2).sum
      if (den == 0) { return (target_id, target_id_avg) }
      (target_id, num / den)
    }
    else{

        val target_id = x._1
        val target_id_avg = avg_dict.getOrElse(target_id, avg_dict(unk_id))

        var result = ListBuffer[Tuple2[Double, Double]]()
        for ((id, score) <- x._2) {
          if (target_id < id) {
            result.append( (score-avg_dict.getOrElse(id, avg_dict(unk_id)), model.getOrElse((target_id, id), 0.0)))
          }
          else { result.append(
            (score-avg_dict.getOrElse(id, avg_dict(unk_id)),model.getOrElse((id, target_id), 0.0)))
          }
        }
        result = result.sortBy(x => -1 * x._2).slice(0, N)
        val num = result.map(x => x._1 * x._2).sum
        if (num == 0) { return (target_id, target_id_avg) }

        val den = result.map(x => x._2).sum
        if (den == 0) { return (target_id, target_id_avg) }
        (target_id, target_id_avg+num/den)
      }
    }

def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss = SparkSession
      .builder()
      .appName("task3train")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val train_input_file = args(0)
    val test_input_file = args(1)
    val model_file = args(2)
    val output_file = args(3)
    val cf_type = args(4)

    val sc = ss.sparkContext

    val train_input = sc.textFile(train_input_file).map(x => JsonMethods.parse(x).values.asInstanceOf[Map[String, Any]] )
      .map( json => (json.getOrElse("business_id", null).asInstanceOf[String],
                    json.getOrElse("user_id", null).asInstanceOf[String],
                    json.getOrElse("stars", null).asInstanceOf[Double]) )

    val unq_business = train_input.map( x=> x._1 ).distinct().collect().toBuffer
    unq_business.append("UNK")
    val buss2idx = mutable.HashMap[String, Int]()
    val idx2buss = mutable.HashMap[Int, String]()
    var i = 0
    for ( buss <- unq_business ){ buss2idx(buss) = i; idx2buss(i) = buss; i = i+1 }

    val unq_users = train_input.map( x=> x._2 ).distinct().collect().toBuffer
    unq_users.append("UNK")
    val user2idx = mutable.HashMap[String, Int]()
    val idx2user = mutable.HashMap[Int, String]()
    i = 0
    for ( usr <- unq_users ){ user2idx(usr) = i; idx2user(i) = usr; i = i+1 }

    val UNK_USER_ID = user2idx("UNK")
    val UNK_BUSS_ID = buss2idx("UNK")

    if (cf_type=="item_based"){
      val model = sc.textFile(model_file)
        .map(x => JsonMethods.parse(x).values.asInstanceOf[Map[String, Any]])
        .map(json => (json.getOrElse("b1", null).asInstanceOf[String],
                      json.getOrElse("b2", null).asInstanceOf[String],
                      json.getOrElse("sim", null).asInstanceOf[Double]) )
        .map(x=> ( buss2idx.getOrElse(x._1, -1) , buss2idx.getOrElse(x._2, -1), x._3  ))
        .filter( x=> (x._1 != -1) | (x._2 != -1) )
        .map( x=> if (x._1 < x._2) ( (x._1, x._2), x._3) else ( (x._2, x._1), x._3 ) )
        .collectAsMap()

      val train = train_input.map( x => (user2idx(x._2), (buss2idx(x._1), x._3)) )
        .groupByKey().mapValues(x=>x.toSet).mapValues( x => takeAvg(x) )

      var temp = train_input.map( x=> (buss2idx(x._1), (x._3, 1)) )
        .reduceByKey( (x,y) => (( x._1+y._1 ), (x._2+y._2)) )
        .mapValues( x=> x._1/x._2.toDouble ).collectAsMap()
      val buss_avg_dict = mutable.HashMap[Int, Double]()
      for ( (k,v) <- temp ){  buss_avg_dict(k) = v  }
      buss_avg_dict(UNK_BUSS_ID) = (1.0*buss_avg_dict.values.sum)/buss_avg_dict.size

      val test2 = sc.textFile(test_input_file).map(x => JsonMethods.parse(x).values.asInstanceOf[Map[String, Any]])
        .map( json => (json.getOrElse("business_id", null).asInstanceOf[String],
                      json.getOrElse("user_id", null).asInstanceOf[String]))

      val not_available = test2.filter( x=> !buss2idx.contains(x._1) | !user2idx.contains(x._2) )
        .map( x => "{"+"\"user_id\": \""+x._2+"\", \"business_id\": \""+x._1+"\", \"stars\":"+buss_avg_dict(UNK_BUSS_ID).toString+"}" )
        .collect()

      val test = test2.filter(x => ( buss2idx.contains(x._1) & user2idx.contains(x._2) ))
        .map(x=> ( user2idx(x._2), buss2idx(x._1) ))

      val output = test.join(train)
        .mapValues( x => predict(x, 5, buss_avg_dict, model, UNK_BUSS_ID, cf_type))
        .map( x => ( idx2user(x._1), idx2buss( x._2._1 ), x._2._2 ) )
        .map( x => "{"+"\"user_id\": \""+x._1+"\", \"business_id\": \""+x._2+"\", \"stars\":"+x._3.toString+"}"  )
        .collect()

      new PrintWriter(output_file) { write( not_available.mkString("\n")+"\n"+output.mkString("\n")); close }
    }else{

      val model = sc.textFile(model_file)
        .map(x => JsonMethods.parse(x).values.asInstanceOf[Map[String, Any]])
        .map(json => (json.getOrElse("u1", null).asInstanceOf[String],
          json.getOrElse("u2", null).asInstanceOf[String],
          json.getOrElse("sim", null).asInstanceOf[Double]) )
        .map(x=> ( user2idx.getOrElse(x._1, -1) , user2idx.getOrElse(x._2, -1), x._3  ))
        .filter( x=> (x._1 != -1) | (x._2 != -1) )
        .map( x=> if (x._1 < x._2) ( (x._1, x._2), x._3) else ( (x._2, x._1), x._3 ) )
        .collectAsMap()

      val train = train_input.map( x => (buss2idx(x._1), (user2idx(x._2), x._3)) )
        .groupByKey().mapValues(x=>x.toSet).mapValues( x => takeAvg(x) )

      var temp = train_input.map( x=> (user2idx(x._2), (x._3, 1)) )
        .reduceByKey( (x,y) => (( x._1+y._1 ), (x._2+y._2)) )
        .mapValues( x=> x._1/x._2.toDouble ).collectAsMap()
      val user_avg_dict = mutable.HashMap[Int, Double]()
      for ( (k,v) <- temp ){  user_avg_dict(k) = v  }
      user_avg_dict(UNK_USER_ID) = (1.0*user_avg_dict.values.sum)/user_avg_dict.size

      val test2 = sc.textFile(test_input_file).map(x => JsonMethods.parse(x).values.asInstanceOf[Map[String, Any]])
        .map( json => (json.getOrElse("business_id", null).asInstanceOf[String],
          json.getOrElse("user_id", null).asInstanceOf[String]))

      val not_available = test2.filter( x=> !buss2idx.contains(x._1) | !user2idx.contains(x._2) )
        .map( x => "{"+"\"user_id\": \""+x._2+"\", \"business_id\": \""+x._1+"\", \"stars\":"+user_avg_dict(UNK_USER_ID).toString+"}" )
        .collect()

      val test = test2.filter(x => ( buss2idx.contains(x._1) & user2idx.contains(x._2) ))
        .map(x=> ( buss2idx(x._1), user2idx(x._2) ))

      val output = test.join(train)
        .mapValues( x => predict(x, 5, user_avg_dict, model, UNK_USER_ID, cf_type))
        .map( x => ( idx2buss(x._1), idx2user( x._2._1 ), x._2._2 ) )
        .map( x => "{"+"\"user_id\": \""+x._2+"\", \"business_id\": \""+x._1+"\", \"stars\":"+x._3.toString+"}"  )
        .collect()

      new PrintWriter(output_file) { write( not_available.mkString("\n")+"\n"+output.mkString("\n")); close }
    }
  }


}
