def fun[T](data : T) = data

val lines = spark.read.textFile("in2.txt").rdd
val links1 = lines.map{ s =>
	val parts = s.split("\\s+")
	(parts(0), parts(1))
}
val links2 = links1.distinct()
val links3 = links2.groupByKey()
val links4 = links3.cache()
var ranks = links4.mapValues(v => 1.0)

for (i <- 1 to 1) {
	val jj = links4.join(ranks)
	println(jj)
	val contribs = jj.values.flatMap{
		case (urls, rank) =>
			urls.map(url => (url, rank / urls.size))
	}
	
ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)

}
val output = ranks.collect()
output.foreach(tup => println(s"${tup._1} has rank: ${tup._2} ."))