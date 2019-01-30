package com.sparkTutorial.scratchpad;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.rank;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class WindowFunction {

	public static void main(String[] args) throws AnalysisException {
		countFunction();
	}

	public static void countFunction() {
		Logger.getLogger("org").setLevel(Level.ERROR);

		SparkSession sparkSession = SparkSession.builder().appName("ScratchPad").master("local[*]").getOrCreate();

		Dataset<Row> stockDF = sparkSession.read().option("header", true).option("inferSchema", true)
				.csv("in/stockData.csv");

		WindowSpec window = Window.partitionBy(stockDF.col("ticker")).orderBy(stockDF.col("open").asc());

		Column column_rank = rank().over(window);

		stockDF.select(col("ticker"), col("open"), column_rank.as("rank")).where(col("rank").leq(1)).show();

//		select distinct * from ( select ticker,open,dense_rank() over (partition by ticker order by open) as rnk from stock_data) s where rnk=1;
		stockDF.select(col("ticker"), col("open"),
				rank().over(Window.partitionBy(stockDF.col("ticker")).orderBy(stockDF.col("open").asc())).alias("rnk"))
				.where(col("rnk").leq(1)).show();

	}
}
