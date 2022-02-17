package com.mfsafrica.recon;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.extern.java.Log;

@SpringBootApplication
@Log
public class SpringSparkAppApplication implements CommandLineRunner {
	public static void main(String[] args) {
		SpringApplication.run(SpringSparkAppApplication.class, args);
	}

	@Override
	public void run(String... args) {
		accessCassandraUsingSparkJavaRdd();
		accessCassandraUsingSparkSql();
	}

	void accessCassandraUsingSparkJavaRdd() {
		SparkConf conf = new SparkConf(true)
				.set("spark.cassandra.connection.host", "127.0.0.1")
				.set("spark.cassandra.auth.username", "cassandra")
				.set("spark.cassandra.auth.password", "cassandra");

		conf.setAppName("Java API demo");
		conf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> rowString = javaFunctions(sc).cassandraTable("mfspoc", "mno_grouping",
				mapColumnTo(String.class)).select("status");

		log.info("accessCassandraUsingRDD :: {}"+rowString.collect().toString());
	}

	void accessCassandraUsingSparkSql() {
		log.info("-----------------------accessCassandraUsingSparkSql :: {}");
		SparkSession sparkCassandra = SparkSession.builder()
				.master("local[*]")
				.appName("SparkCassandraApp")
				// .config("spark.sql.catalog.mycatalog",
				// "com.datastax.spark.connector.datasource.CassandraCatalog")
				.config("spark.cassandra.connection.host", "localhost")
				.config("spark.cassandra.connection.port", "9042")
				.config("spark.cassandra.auth.username", "cassandra")
				.config("spark.cassandra.auth.password", "cassandra")
				// .config("spark.submit.deployMode", "cluster")
				.getOrCreate();

		Dataset<Row> casDataset = sparkCassandra.sqlContext()
				.read()
				.format("org.apache.spark.sql.cassandra")
				.option("keyspace", "mfspoc")
				.option("table", "mno_grouping")
				.load();

		// Dataset<Row> casDataset = sparkCassandra.sql("select * from
		// mycatalog.mfspoc.mno_grouping");

		casDataset.show();
	}

}
