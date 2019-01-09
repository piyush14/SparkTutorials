package com.sparkTutorial.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ConnectToKafkaTopicBatch {

	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);		
		SparkSession sparkSession = new SparkSession.Builder().appName("ConnectToKafkaTopicBatch").master("local[*]").getOrCreate();

		Dataset<Row> df = sparkSession.read().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "test_topic_1").load();

		Dataset<Row> df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
		
		df1.select("value").show();
		System.out.println(df.collect().toString());
	}

}
