package com.sparkTutorial.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class ConnectToKafkaTopicStreaming {

	public static void main(String[] args) throws StreamingQueryException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkSession sparkSession = new SparkSession.Builder().appName("ConnectToKafkaTopicStreaming").master("local[*]").getOrCreate();

		Dataset<Row> df = sparkSession.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "test_topic_1").load();

//		while (true)
		Dataset<Row> df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

		StreamingQuery query = df1.select("value").writeStream().outputMode("append").format("console").start();

		query.awaitTermination();
	}

}
