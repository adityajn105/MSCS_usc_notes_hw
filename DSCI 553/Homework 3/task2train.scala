import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods

import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

object task2train {

  //spark-submit --driver-memory 4g --executor-memory 4g --class task2train hw2.jar $ASNLIB/publicdata/train_review.json task2.scala.model $ASNLIB/publicdata/stopwords

  def preprocess(json_text: String, stopwords: Set[String]): Tuple2[String, String] ={
    val json_dict = JsonMethods.parse(json_text).values.asInstanceOf[Map[String, Any]]

    var text = json_dict.getOrElse("text", null).asInstanceOf[String].toLowerCase()
    text = text.replaceAll("[\\(\\[,\\].!?:;\\)\\-&*%$0-9\\'~]", " ")

    val words = ListBuffer[String]()
    for( word <- text.split(" ").toList ){
      if(!stopwords.contains(word)){ words.append(word) }
    }
    val b_id = json_dict.getOrElse("business_id", null).asInstanceOf[String]
    (b_id, words.mkString(" "))
  }

  def term_frequency(words: List[String]): mutable.ListBuffer[ Tuple2[String, Double] ] ={
    val count = mutable.HashMap[String, Double]()
    var mx_freq = 0
    for (word <- words){
      count(word) = 1.0 + count.getOrElse(word, 0.0)
      mx_freq = Integer.max( mx_freq, count(word).toInt )
    }

    val items = mutable.ListBuffer[ Tuple2[String, Double] ]()
    for ( (k,v) <- count ){ items.append( (k, (1.0*v)/mx_freq) ) }
    items
  }

  def calc_tfidf( x: ListBuffer[Tuple2[String, Double]], idf: scala.collection.Map[String, Double] ): List[String] ={
    var tfidf = ListBuffer[ Tuple2[ Double, String ] ]()
    for ( (word, freq) <- x ){
      tfidf.append( ( freq*idf(word), word ) )
    }

    tfidf = tfidf.sortBy( x => -1*x._1 ).slice(0, 200)
    tfidf.map(x=> x._2).toList
  }

  def replaceWithIndex(words: List[String], w2i: mutable.HashMap[ String, Int ]): mutable.Set[Int] ={
    val ans = mutable.Set[Int]()
    for (word <- words){  ans.add(w2i(word))}
    ans
  }

  def preprocess_user_business(json_text: String): Tuple2[String, immutable.Set[String]] ={
    val json_dict = JsonMethods.parse(json_text).values.asInstanceOf[Map[String, Any]]
    val value = immutable.Set[String]( (json_dict.getOrElse("business_id", null).asInstanceOf[String]) )
    ( json_dict.getOrElse("user_id", null).asInstanceOf[String], value )
  }

  def createUserProfile(values: Set[String], documentProfile: scala.collection.Map[String, mutable.Set[Int]]): mutable.Set[Int] ={
    var userProfile = mutable.Set[Int]()
    for (bid <- values){
      userProfile = userProfile.union(documentProfile(bid))
    }
    userProfile
  }

  def removeRareWords(words: List[String], rarewords: Set[String]): ListBuffer[String] ={
      val finalwords = ListBuffer[String]()
      for (word <- words ){
        if ( !rarewords.contains(word) ){ finalwords.append(word) }
      }
    finalwords
  }


  def tfidf_helper( words: List[String]): ListBuffer[Tuple2[String, Int]] = {
    val ans = ListBuffer[Tuple2[String,Int]]()
    for (word: String <- words.toSet){
      ans.append( (word, 1) )
    }
    ans
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
    val stopwords_file = args(2)

    val sc = ss.sparkContext
    var stopwords = sc.textFile(stopwords_file).map(x=>x).collect().toSet
    stopwords = stopwords.union( mutable.Set( ("") ))

    var business_review = sc.textFile(input_file)
      .map(x=>preprocess(x, stopwords))
      .reduceByKey( (x,y) => x+" "+y )
      .mapValues( x=> x.split(" ").toList )

    val TOTAL_DOCS = business_review.count()
    val wordCount = business_review.flatMap( x => x._2.map( y => (y,1) ) ).reduceByKey( (x,y) => x+y )

    val TOTAL_NO_OF_WORDS = wordCount.map( x => x._2 ).reduce((x,y)=>x+y)
    var rarewords = wordCount.filter( x => x._2 < TOTAL_NO_OF_WORDS*0.000001)
      .map(x=>x._1).collect().toSet
    business_review = business_review.mapValues( words => removeRareWords(words, rarewords).toList)

    val term_freq = business_review.mapValues( x => term_frequency(x) )
    val idf = business_review.flatMap( x =>  tfidf_helper(x._2) )
      .reduceByKey( (x,y) => x+y )
      .mapValues( x => (1.0*math.log(TOTAL_DOCS/x))/math.log(2) )
      .collectAsMap()

    val tfidf = term_freq.mapValues( x => calc_tfidf(x, idf) )
    val words = tfidf.flatMap( x => x._2  ).collect().toSet

    val word2idx = mutable.HashMap[ String, Int ]()
    var i=0
    for (word <- words){ word2idx(word) = i; i = i+1 }

    val documents = tfidf.mapValues(x => replaceWithIndex( x, word2idx))
    val documentProfileMap = documents.collectAsMap()

    val documentsProfile = documents.map(
      x => "{"+"\"type\": \"Document\", \"id\": \""+x._1+"\", \"value\": ["+x._2.toList.mkString(", ")+"] }")
      .collect()

    val userprofile = sc.textFile(input_file)
      .map( x=> preprocess_user_business(x) )
      .reduceByKey( (x,y) => x.union(y))
      .mapValues(values => createUserProfile(values, documentProfileMap))
      .map( x => "{"+"\"type\": \"User\", \"id\": \""+x._1+"\", \"value\": ["+x._2.toList.mkString(", ")+"] }" )
      .collect()

    val ans = documentsProfile.mkString("\n")+"\n"+userprofile.mkString("\n")
    new PrintWriter(model_file) { write(ans); close }
  }
}
