package org.devoxx.spark.lab.devoxx2015;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.log4j.BasicConfigurator;
import org.apache.spark.sql.functions;



public class Workshop3 {

	
	
    public static void main(String... args){
  
    	BasicConfigurator.configure();
    	System.setProperty("hadoop.home.dir", "d:\\winutil\\");

    	SparkSession spark = SparkSession
    			  .builder()
    			  .appName("Java Spark SQL basic example")
    			  .config("spark.master", "local")
    			  .getOrCreate(); 
    	

    	Dataset<Row> people = spark.read().format("csv")  .option("header", "true").option("inferSchema", "true").load("dataset.csv");
    	people.printSchema();
    	people.createOrReplaceTempView("people");
 
    	
    	Dataset<Row>  namesDF = people.withColumn("rectal_temp",people.col("rectal_temp").cast("Decimal(18)"));
  
    	namesDF.write().format("parquet").save("newdataset.parquet"); 	
    	
    	namesDF.show();
    	

    }
}