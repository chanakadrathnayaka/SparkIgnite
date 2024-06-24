package com.iamchanaka.sparkignitetest.services;

//import org.apache.ignite.spark.IgniteContext;
//import org.apache.ignite.spark.IgniteRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

@Service
public class SparkService {

  private final SparkSession sparkSession = null;
//  private final IgniteContext igniteContext;

/*
  public SparkService(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
//    this.igniteContext = new IgniteContext(this.sparkSession.sparkContext(), "ignite-config.xml");
  }
*/

  public void createDataSet() {
    Dataset<Row> asd = sparkSession.read()
        .option("header", "true")
        .parquet("/Users/chanaka/Projects/Java/SparkIgniteTest/data/die_t2.parquet");
    asd.show();
//    asd.map()
//    IgniteRDD<Integer, String> a = this.igniteContext.fromCache("sparkCache");
//    a.savePairs();

    try {
      asd.createTempView("sampleData");
    } catch (AnalysisException e) {
      throw new RuntimeException(e);
    }
  }

  public void readDataSet() {
    Dataset<Row> asd2 = sparkSession.sql("select * from sampleData");
    asd2.printSchema();
  }
}
