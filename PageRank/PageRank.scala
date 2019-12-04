import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def main(args:Array[String]): Unit ={
    if(args.length != 2){
      println("Usage: <in> <out>")
      return
    }

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val links = lines.map(line=>{
      val parts = line.split("\\s")
      (parts(0), parts(1))
    }).distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.00)

    for(i <- 1 to 10){
      val contrib = links.join(ranks).values.flatMap {
        case (urls, rank) => {
          val size = urls.size
          urls.map(url => (url, rank/size))
        }
      }
      ranks = contrib.reduceByKey(_+_).mapValues(0.15+0.85*_)
    }
    ranks.sortByKey(false).saveAsTextFile(args(1))
  }
}

