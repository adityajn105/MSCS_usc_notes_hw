import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, immutable, mutable}
import scala.util.Random
import java.io.File
import scala.io.Source
import org.apache.commons.io.FileUtils
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object bfr {

  def getDataLoad(file: String): List[ List[Double] ] = {
    var data = ListBuffer[List[Double]]()
    for (line <- Source.fromFile(file).getLines() ) {
      data.append( line.split(",").map(x => x.toDouble).toList )
    }
    data = Random.shuffle(data)
    data.toList
  }

  def euclidean(x1: List[Double], x2: List[Double]): Double = {
    var ans = 0.0
    for( (i1,i2) <- x1 zip x2 ){ ans = ans + math.pow(i1-i2, 2) }
    ans
  }

  def argmin( x : List[Double]): Int ={
    val n = x.length
    var (min_x, min_i) = (Integer.MAX_VALUE.toDouble, 0)
    for ( (i,xi)  <- (0 to n-1) zip x ){
      if ( xi < min_x ){
        min_x = xi
        min_i = i
      }
    }
    min_i
  }

  def add_vector( v1: List[Double], v2: List[Double] ): List[Double] = {
    val v = ListBuffer[Double]()
    for ( (i,j) <- v1 zip v2 ){
      v.append(i+j)
    }
    v.toList
  }

  def cluster_changed(old: List[List[Double]], neww: List[List[Double]]): Boolean = {
    var change = 0.0
    for ( (o,n) <- old zip neww ){
      for ( (oi, ni) <- o zip n) {
        change = change + math.abs(oi-ni)  //if (oi!=ni) { return true }
      }
    }
    change > 0.01
  }

  def initialize_cluster(x: List[List[Double]], size: Int): List[List[Double]] = {
    Random.shuffle(x).take(size).map(x=> x.slice(1,x.length))
  }

  def kmeans_fit(sc: SparkContext,
                 data : List[List[Double]],
                 k: Int,
                 max_iters: Int = 30 )
        : (Map[String, Int], Map[Int, (Int, List[Double], List[Double])],
           Map[Int, List[(String, List[Double])]] ) = {

    var max_iterations = max_iters
    var cluster_centers = initialize_cluster(data, k)
    val initial = sc.parallelize(data).map( x=> (x(0).toInt.toString, x.slice(1, x.length) ) )
    var point_cluster = Map[String, Int]()

    while( max_iterations != 0 ){
      point_cluster = initial.mapValues(x => cluster_centers.map(center => euclidean(x, center)) )
       .mapValues( x=> argmin(x) ).collectAsMap()

      var new_cluster_centers_rdd = initial.map(x=> ( point_cluster(x._1), (x._2, 1)) )
        .reduceByKey( (x,y) => ( add_vector(x._1, y._1), x._2+y._2 ) )
        .mapValues( x => x._1.map( y => y/x._2 )  )

      var new_cluster_centers = new_cluster_centers_rdd.collect().sortWith( (x,y) => x._1 < y._1 )
        .map( x => x._2).toList

      if ( cluster_changed(cluster_centers, new_cluster_centers) ){
          cluster_centers = new_cluster_centers
      }
      else{
          cluster_centers = new_cluster_centers
          max_iterations = 1
      }
      max_iterations = max_iterations - 1
    }

    val summary = initial.mapValues(x=> (cluster_centers.map(center => euclidean(x, center)), x))
      .map(x=> (argmin( x._2._1), (1, x._2._2, x._2._2.map( v => v*v )) ))
      .reduceByKey( (x,y) => (x._1+y._1, add_vector(x._2, y._2), add_vector(x._3, y._3)) )
      .collectAsMap()

    val cluster_points = initial.map(
      x => ( argmin(cluster_centers.map(center => euclidean(x._2, center))), (x._1, x._2) )
    ).groupByKey().mapValues(x=>x.toList).collectAsMap()
    (point_cluster, summary, cluster_points)
  }

  def seperate_retained(ans: Map[String, Int], summary: Map[Int, (Int, List[Double], List[Double])],
                        cluster_points: Map[Int, List[(String, List[Double])]]):
  (Map[String, Int], Map[Int, (Int, List[Double], List[Double])], Map[Int, List[(String, List[Double])]], ListBuffer[List[Double]]) ={
    val retained = ListBuffer[List[Double]]()
    val new_summary = mutable.HashMap[Int, (Int, List[Double], List[Double])]()
    for( (k,v) <- summary ){  new_summary(k) = v  }

    val new_ans = mutable.HashMap[String, Int]()
    for( (k,v) <- ans ){  new_ans(k) = v  }

    val new_cluster_points = mutable.HashMap[Int, List[(String, List[Double])]]()
    for( (k,v) <- cluster_points ){  new_cluster_points(k) = v  }

    for ( (k,v) <- summary){
      if ( v._1 <= 1){
        if (v._1 == 1){
          var kv = cluster_points(k)(0)
          var temp = ListBuffer[Double]( kv._1.toDouble )
          for ( value <- kv._2 ){ temp.append(value) }
          retained.append(temp.toList)
          new_ans.remove(kv._1)
        }
        new_summary.remove(k)
        new_cluster_points.remove(k)
      }
    }
    (new_ans.toMap, new_summary.toMap, new_cluster_points.toMap, retained)
  }

  def getInteriorRetained(cluster_points:Map[Int, List[(String, List[Double])]], t:Int=10 ):
        (List[List[Double]], ListBuffer[List[Double]]) = {
    val interior = ListBuffer[List[Double]]()
    val retained = ListBuffer[List[Double]]()
    for ( (_, ls) <- cluster_points ){
      for ( (key, value) <- ls){
        var temp = ListBuffer[Double]( key.toDouble )
        for ( v <- value){ temp.append(v) }

        if (ls.length < t) { retained.append(temp.toList) }
        else{ interior.append(temp.toList)}
      }
    }
    (interior.toList, retained)
  }

  def reformat(key:String, values:List[Double]): List[Double]={
    var temp = ListBuffer[Double]( key.toDouble )
    for ( v <- values){ temp.append(v) }
    temp.toList
  }


  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val input_dir = args(0)
    val n_clusters = Integer.parseInt(args(1))
    val intermediate_results = args(3)
    val output_file = args(2)

    val alpha = 2
    var round = 1
    val files = new File(input_dir).listFiles.map(x => x.getName).sortWith((x,y) => x < y)
      .map( y => input_dir+"/"+y ).toList

    var discard_set_summary = mutable.HashMap[Int, (Int,List[Double],List[Double])]()
    var compression_set_summary = mutable.HashMap[Int, (Int,List[Double],List[Double])]()
    var discard_answer = mutable.HashMap[String, Int]()
    var compression_answer = mutable.HashMap[String, Int]()
    var retained_set = ListBuffer[List[Double]]()

    val intermediates = ListBuffer[String]()
    intermediates.append("round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained")

    val ss = SparkSession.builder().appName("bfr").config("spark.master", "local[*]").getOrCreate()
    val sc = ss.sparkContext

    var data = getDataLoad(files(0))
    val d = data(0).length - 1
    val threshold = alpha*math.sqrt(d)

    val fraction = (data.length * 0.4).toInt
    val sample = data.slice(0, fraction)

    var (_, _, cluster_points) = kmeans_fit( sc, sample, n_clusters*5)
    val (interior, retained) = getInteriorRetained((cluster_points))

    for (v <- retained){retained_set.append(v)}

    val (ds_ans, ds_summary, _) = kmeans_fit(sc, interior, n_clusters)
    for ((k,v) <- ds_ans){ discard_answer(k) = v}
    for ((k,v) <- ds_summary){ discard_set_summary(k) = v }

    val rest_data = data.slice(fraction, data.length)
    var (point_cluster, summary, cluster_pointss) = kmeans_fit(sc, rest_data, n_clusters*3)

    val (cs_map, cs_summary, _, retain) = seperate_retained(point_cluster, summary, cluster_pointss)
    for ((k,v) <- cs_map){ compression_answer(k) = v}
    for ((k,v) <- cs_summary){ compression_set_summary(k) = v }

    for (v <- retain){retained_set.append(v)}

    intermediates.append(round+","+discard_set_summary.size+","+discard_answer.size+","+compression_set_summary.size+","+compression_answer.size+","+retained_set.size)

    for ( file <- files.slice(1, files.length) ){
      data = getDataLoad(file)
      var rdd = sc.parallelize(data).map(x => (x(0).toInt.toString, x.slice(1, x.length)))
        .map( x => ( assign_to_cluster(x._2, threshold, ds_summary), x._1, x._2  ) )

      var temp = rdd.filter(x=> x._1 != -1).map(x=> (x._2, x._1)).collectAsMap()
      for ( (k,v) <- temp ){ discard_answer(k) = v}

      var ds_updates = rdd.filter(x=> x._1 != -1).map(x=> (x._1, (1, x._3, x._3.map(y => math.pow(y,2)))  ))
        .reduceByKey( (x,y) => ( x._1+y._1, add_vector(x._2, y._2), add_vector(x._3, y._3) ))
        .collectAsMap()
      for ( (k,v) <- ds_updates ){ discard_set_summary(k) = v }

      var cs_rdd = rdd.filter( x => x._1 == -1 ).map( x => (x._2, x._3) )
        .map( x=> (assign_to_cluster(x._2, threshold, cs_summary), x._1, x._2) )
      temp = cs_rdd.filter(x=>x._1 != -1).map(x=>(x._2, x._1)).collectAsMap()
      for ( (k,v) <- temp ){ compression_answer(k) = v}

      var cs_update = cs_rdd.filter(x=>x._1 != -1).map(x=>(x._1, (1, x._3, x._3.map(y => math.pow(y,2)))))
        .reduceByKey( (x,y) => (x._1+y._1, add_vector(x._2, y._2), add_vector(x._3, y._3)) )
        .collectAsMap()
      for ( (k,v) <- cs_update ){ compression_set_summary(k) = v }

      for( v <- cs_rdd.filter(x=> x._1 == -1).map( x=> reformat(x._2, x._3) ).collect() ){
        retained_set.append(v)
      }

      if (retained_set.length >= 3*n_clusters){
        var (point_cluster, summary, cluster_points) = kmeans_fit(sc, retained_set.toList, n_clusters*3)
        var output = seperate_retained(point_cluster, summary, cluster_points)
        retained_set = output._4
        var temp = updateCS(compression_answer, compression_set_summary, output._1, output._2)
        compression_answer = temp._1
        compression_set_summary = temp._2
      }

      val merged_cs = mergeCS(threshold, compression_answer, compression_set_summary)
      compression_answer = merged_cs._1
      compression_set_summary = merged_cs._2

      round = round+1
      intermediates.append(round+","+discard_set_summary.size+","+discard_answer.size+","+compression_set_summary.size+","+compression_answer.size+","+retained_set.size)
    }

    val new_cluster_map = mutable.HashMap[Int, Int]()
    for ( (key, summary) <- compression_set_summary){
      var center = summary._2.map( v => v/summary._1 )
      var best = ( Integer.MAX_VALUE.toDouble, 0 )
      for ( (ds_cluster_idx, ds_sum) <- discard_set_summary ){
        var mh = mahalanobis_distance( d, center, ds_sum._1, ds_sum._2, ds_sum._3 )
        if (mh < best._1){ best = (mh, ds_cluster_idx) }
      }
      new_cluster_map(key) = best._2
    }

    for ( (idx, old_cluster) <- compression_answer ){
      compression_answer(idx) = new_cluster_map(old_cluster)
    }
    for ( (k,v) <- compression_answer ){ discard_answer(k) = v }

    for ( (k,v) <- sc.parallelize(retained_set).map( x => (x(0).toInt.toString, -1) ).collectAsMap()){
      discard_answer(k) = v
    }

    new PrintWriter(intermediate_results) { write(intermediates.mkString("\n")); close }

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    FileUtils write( new File(output_file), mapper.writeValueAsString(discard_answer) )
  }

  def mergeCS(threshold: Double, mapp: mutable.HashMap[String, Int], summary:  mutable.HashMap[Int, (Int, List[Double], List[Double])]): (mutable.HashMap[String, Int], mutable.HashMap[Int, (Int, List[Double], List[Double])])={
    val new_summary = mutable.HashMap[Int, (Int, List[Double], List[Double])]()
    val new_cmap = mutable.HashMap[Int, Int]()
    val avail = mutable.HashSet[Int]()
    for ( k <- summary.keys){avail.add(k)}
    var i = 0
    for ( key <- summary.keys.toList.combinations(2) ){
      var (k1,k2) = (key(0), key(1))
      if (avail.contains(k1) & avail.contains(k2)){
        var centroid1 = summary(k1)._2.map( v=> v/summary(k1)._1 )
        var centroid2 = summary(k2)._2.map( v=> v/summary(k2)._1 )
        var mh1 = mahalanobis_distance( centroid1.length, centroid1, summary(k2)._1, summary(k2)._2, summary(k2)._3  )
        var mh2 = mahalanobis_distance( centroid2.length, centroid2, summary(k1)._1, summary(k1)._2, summary(k1)._3 )
        var mh = if (mh1 > mh2) mh1 else mh2

        if (mh < threshold){
          new_cmap(k1) = i
          new_cmap(k2) = i
          avail.remove(k1)
          avail.remove(k2)
          new_summary(i) = ( summary(k1)._1 + summary(k2)._1,
            add_vector(summary(k1)._2, summary(k2)._2),
            add_vector(summary(k1)._3, summary(k2)._3) )
        }
      }
    }

    for ( (k,v) <- mapp ){
      if (!new_cmap.contains(v)){
        new_cmap(v) = i
        new_summary(i) = summary(v)
        i = i+1
      }
      mapp(k) = new_cmap(v)
    }
    (mapp, new_summary)
  }

  def mahalanobis_distance(d: Int, point: List[Double], N: Int, SUM: List[Double], SUMSQ: List[Double] ): Double = {
    var mh = 0.0
    for ( i <- (0 to d-1) ){
      var std = math.sqrt( (SUMSQ(i)/N) - math.pow(SUM(i)/N, 2) )
      var centroid = SUM(i)/N
      var normalized = if (std==0) (point(i)-centroid) else (point(i)-centroid)/std
      mh = mh + math.pow(normalized, 2)
    }
    math.sqrt(mh)
  }

  def assign_to_cluster(point: List[Double], threshold: Double, summary: Map[Int, (Int, List[Double], List[Double])]): Int={
    var (min_idx, min_mh) = (0, Integer.MAX_VALUE.toDouble)
    for ( (idx,summ) <- summary ){
      var N = summ._1
      var SUM = summ._2
      var SUMSQ = summ._3

      var mh =  mahalanobis_distance( SUM.length, point, N, SUM, SUMSQ )
      if (mh < min_mh){
        min_mh = mh
        min_idx = idx
      }

    }
    if (min_mh < threshold) { min_idx}
    else{ -1 }
  }

  def updateSummary( old_sum: mutable.HashMap[Int, (Int, List[Double], List[Double])], updates: Map[Int, (Int, List[Double], List[Double])] )
                  : mutable.HashMap[Int, (Int, List[Double], List[Double])]= {
    for ( (idx, summary) <- updates ){
      old_sum(idx) = ( old_sum(idx)._1 + updates(idx)._1,
                          add_vector( old_sum(idx)._2, old_sum(idx)._2),
                          add_vector( old_sum(idx)._3, old_sum(idx)._3) )
    }
    old_sum
  }

  def updateCS( cs_ans: mutable.HashMap[String, Int], cs_summary: mutable.HashMap[Int, (Int, List[Double], List[Double])],
                point_cluster: Map[String, Int], summary: Map[Int, (Int, List[Double], List[Double])])
        : (mutable.HashMap[String, Int], mutable.HashMap[Int, (Int, List[Double], List[Double])])={

      val max_idx = cs_summary.keys.max
      for ( (pt, cluster) <- point_cluster ){cs_ans(pt) = cluster+max_idx}

      for ( (cluster, summ) <- summary ){cs_summary( cluster+max_idx ) = summ}
    (cs_ans, cs_summary)
  }

}
