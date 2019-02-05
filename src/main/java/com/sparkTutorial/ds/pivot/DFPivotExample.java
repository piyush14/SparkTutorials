package com.sparkTutorial.ds.pivot;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DFPivotExample {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().appName("ScratchPad").master("local[*]").getOrCreate();

		Logger.getLogger("org").setLevel(Level.ERROR);

		Dataset<Row> membersDF = sparkSession.read().option("header", true).option("inferSchema", true)
				.csv("in/members.csv");

		Dataset<Row> moviesDF = sparkSession.read().option("header", true).option("inferSchema", true)
				.csv("in/movies.csv");

		Dataset<Row> moviesDFCopy = sparkSession.read().option("header", true).option("inferSchema", true)
				.csv("in/movies_copy.csv");

//		moviesDF.withColumn("new_col", functions.lit(0)).show();;

//		moviesDF.union(membersDF).show();

//		moviesDF.union(membersDF).distinct().show();

//		System.out.println(moviesDF.toJavaRDD().cartesian(moviesDFCopy.toJavaRDD()));

//		moviesDF.intersect(moviesDFCopy.drop("year")).sort("id").show();

//		moviesDF.select(moviesDF.col("*")).groupBy(moviesDF.col("category")).count().show();

//		na().fill(0) replaces all NULL valus with 0
		moviesDF.groupBy(moviesDF.col("category")).pivot("year").count().na().fill(0).show();
	}

}
