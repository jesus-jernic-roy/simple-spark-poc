package com.asirtech.spark.simple_spark;

import java.sql.SQLException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Read the Csv file and transform into Dataset.
 *
 */
public class ReadCsv {
	public static void main(String csv[]) throws SQLException {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();

		// Schema for the Employee.csv file
		StructType emp_schema = new StructType().add("emp_id", "string").add("first_name", "string")
				.add("last_name", "string").add("addressid", "string").add("language", "string");

		// Schema for the Address.csv file
		StructType ad_schema = new StructType().add("location", "string").add("state", "string")
				.add("addressid","long");

		String filePath1 = "src//main//resource//Employee.csv";
		String filePath2 = ReadCsv.class.getResource("/Address.csv").getPath();

		// Dataset for Employee.csv
		Dataset<Row> dataset1 = sparkSession.sqlContext().read().format("com.databricks.spark.csv")
				.option("header", true).schema(emp_schema).load(filePath1);

		//Dataset for Address.csv
		Dataset<Row> dataset2 = sparkSession.sqlContext().read().format("com.databricks.spark.csv")
				.option("header", true).schema(ad_schema).load(filePath2);

		System.out.println("Dataset1: ");
		dataset1.show();
		dataset1.printSchema();
		System.out.println("DataSet2: ");
		dataset2.show();
		dataset2.printSchema();
		
		// creates temporary table name
		dataset1.createOrReplaceTempView("employee");
		dataset2.createOrReplaceTempView("address");

		//Querying to perform Join operation on the Datasets
		Dataset<Row> sqlQuery = sparkSession.sql("SELECT employee.emp_id, employee.first_name, employee.last_name, "
				+ "employee.addressid, address.location, address.state FROM employee "
				+ "INNER JOIN address ON employee.addressid = address.addressid");
		sqlQuery.show();

    	dataset1.join(dataset2,dataset1.col("addressid").equalTo(dataset2.col("addressid")),"Inner").show();

		Dataset<Row> d1 = dataset1.select(dataset1.col("first_name"));
		d1.show();
		d1.printSchema();

	}
}
