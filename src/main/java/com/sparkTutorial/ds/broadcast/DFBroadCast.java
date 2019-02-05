package com.sparkTutorial.ds.broadcast;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class DFBroadCast {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().appName("ScratchPad").master("local[*]").getOrCreate();
		JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

		Logger.getLogger("org").setLevel(Level.ERROR);
		Dataset<Row> localDF = sparkSession.read().json("in/us_states.json");
		System.out.println(localDF.schema());

		Broadcast<List<Row>> broadcastStateData = sc.broadcast(localDF.collectAsList());
		Broadcast<StructType> broadcastSchema = sc.broadcast(localDF.schema());

		Dataset<Row> storesDF = sparkSession.read().json("in/store_locations.json");
		Dataset<Row> stateDF = sparkSession.createDataFrame(broadcastStateData.value(), broadcastSchema.value());

		System.out.println("How many stores are in each US region?");
		
		Dataset<Row> joinedDF = storesDF.join(stateDF, "state").groupBy("census_region").count();
		joinedDF.show();
	}

}
