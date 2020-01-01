
import org.apache.log4j.{Level,Logger}
import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source 

// 2
def addRatings(path:String):Seq[Rating] = {
	val lines = Source.fromFile(path).getLines()
	val ratings = lines.map{
		line => 
		val fields = line.split("::")
		Rating(fields(0).toInt,fields(1).toInt,fields(2).toDouble)
	}.filter(_.rating > 0.0)
	if(ratings.isEmpty){
		sys.error("No ratings provived")
	}else{
		ratings.toSeq
	}
}
	val myRatings = addRatings("/home/hadoop/rs/rs_movie/ml-1m/person.txt")
	val myRatingsRDD = sc.parallelize(myRatings, 1)


//3 ratings.dat(评分数据)，格式为： 用户Id::电影Id::评分::时间
val ratings = sc.textFile("/home/hadoop/rs/rs_movie/ml-1m/ratings.dat"),map{
	lines =>
		val fields = line.split("::")
		(fields(3).toLong % 10,Rating(fields(0).toInt,fields(1).toInt, fields(2).toDouble ))
}

// movies.dat(电影资源数据)，格式为： 电影id::电影名称::电影类型 
//4.加载movies.dat，取字段(movieId, movieName)。
val movies = sc.textFile("/home/hadoop/rs/rs_movie/ml-1m/movies.dat").map{
	lines =>
		val fields = line.split("::")
		(fields(0).toInt, fields(1))
}.collect().toMap

//5.统计用户数量和电影数量以及用户对电影的评分数目。
val numRatings = ratings.count()
val numUsers = ratings.map(_._2.user).distinct().count()
val numMovies = rating.map(_._2.product).distinct().count()

//6 将样本评分表以 key 值切分成3个部分，分别用于训练(55%，并加入用户评分), 校验 (15%), 测试(30%)，该数据在计算过程中要多次应用到，这里用cache缓存。
val numPartitions = 4
val training = rating.filter(x=>x._1 < 6).values.union(myRatingsRDD).repartition(numPartitions).cache()
val validation = ratings.filter(x=>x._1>=6&&x.1<8).values.repartition(numPartitions).cache()
val test = ratings.filter(x=>x._1>=8).values.cache()

val numTraining = training.count()
val numValidation = validation.count()
val numTest = test.count()

//7.训练不同参数下的模型，迭代次数,根据机器情况设定,并在校验集中验证，获取最佳参数下的模型。
val ranks = List(8, 12)
val lambdas = List(0,1, 10,0)
val numIters = List(10, 20)
var bestModel:Option[MatrixFactorizationModel] = None
var bestValidationRmse = Double.MaxValue
var bestRank = 0
var bestLambda = -1.0
var bestNumIter = -1

//8.定义 compute 函数校验集预测数据和实际数据之间的均方根误差。
def compute(model:MatrixFactorizationModel,data:RDD[Rating],n:Long):Double = {
	val predictions:RDD[Rating] = model.predict((data.map(x=>(x.user,x.product))))
	val predictionsAndRatings = predictions.map{x=>((x.user,x.product),x.rating) }
		.join(data.map(x=>((x.user,x.product),x.rating))).values

	math.sqrt(predictionsAndRatings.map(x=>(x._1-x._2)*(x._1-x._2)  ).reduce(_+_)/n )
}

//9.三层嵌套循环，会产生8个ranks ，lambdas ，iters 的组合，每个组合都会产生一个模型，计算8个模型的方差，最小的那个记为最佳模型。
for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
	val model = ALS.train(training,rank, numIter, lambda)
	val validRmse = compute(model, validation, numValidation)
	if(validRmse <bestValidationRmse ){
		bestModel = Some(model)
		bestValidationRmse = validRmse
		bestRank = rank
		bestLambda = lambda
		bestNumIter = numIter
	}
}

//10.用最佳模型预测测试集的评分，并计算和实际评分之间的均方根误。
val tRm = compute(bestModel.get, test, numTest)

//11.计算最佳模型与原始基础的相比其提升度。
val inspopular = training.union(validation).map(_.rating).mean 
val bRm = math.sqrt(test.map(x=>(inspopular-x.rating)*(inspopular-x.rating)).reduce(_+_)/numTest)
val improvement = (bRm-tRm)/bRm * 100

//12.推荐前15部最感兴趣的电影给我，注意要剔除用户(我)已经评分的电影。
val myinterestedIds = myRatings.map(_.product).toSet
val choice = sc.parallelize(movies.keys.filter(!myinterestedIds.contains(_)).toSeq)
val i = 1

bestModel.get.predict(choice.map((0,_))).collect.sortBy(-_.rating).take(15).foreach {
	r => println("%2d".format(i)+": "+movies(r.product))
	i +=1
}


