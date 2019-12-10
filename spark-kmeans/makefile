all:
	@sbt compile
	@sbt package
submit:
	@spark-submit target/scala-2.11/spark-kmeans_2.11-1.0.jar
run: all submit
