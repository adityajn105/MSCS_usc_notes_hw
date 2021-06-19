import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods

import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

object task2predict {

  //spark-submit --driver-memory 4g --executor-memory 4g --class task2predict hw3.jar $ASNLIB/publicdata/test_review.json task2.scala.model task2.scala.predict
  def cosineSimilarity( bid: String, uid: String,
                        userProfiles:  scala.collection.Map[String, Set[Int]],
                        businessProfiles: scala.collection.Map[String, Set[Int]]): Double ={

    if ( (userProfiles.getOrElse(uid, null) == null) | (businessProfiles.getOrElse(bid, null) == null)) {0.0}
    else{
      val uv = userProfiles(uid)
      val bv = businessProfiles(bid)
      (1.0*uv.intersect(bv).size) / (math.sqrt(uv.size) * math.sqrt(bv.size))
    }
  }


  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val ss = SparkSession
      .builder()
      .appName("task2train")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val input_file = args(0)
    val model_file = args(1)
    val output_file = args(2)

    val sc = ss.sparkContext

    val model = sc.textFile(model_file).map( x => JsonMethods.parse(x).values.asInstanceOf[Map[String, Any]] )
      .map( x => ( (x.getOrElse("type", null).asInstanceOf[String],
                    x.getOrElse("id", null).asInstanceOf[String],
                    x.getOrElse("value", null).asInstanceOf[List[Int]].toSet)))

    val userProfiles = model.filter( x => x._1 == "User" )
      .map(x=> (x._2, x._3))
      .collectAsMap()

    val docProfiles = model.filter( x => x._1 == "Document" )
      .map( x => (x._2, x._3) )
      .collectAsMap()

    val predicted = sc.textFile(input_file)
      .map( x => JsonMethods.parse(x).values.asInstanceOf[Map[String, Any]] )
      .map( x => ( x.getOrElse("business_id", null).asInstanceOf[String],
                    x.getOrElse("user_id", null).asInstanceOf[String]))
      .map( x=> ( x._1, x._2, cosineSimilarity(x._1, x._2, userProfiles, docProfiles) ) )
      .filter( x => x._3 >= 0.01 )
      .map( x => "{"+"\"user_id\": \""+x._2+"\", \"business_id\": \""+x._1+"\", \"sim\": "+x._3.toString+"}" )
      .collect()

    new PrintWriter(output_file) { write(predicted.mkString("\n")); close }
  }

}
