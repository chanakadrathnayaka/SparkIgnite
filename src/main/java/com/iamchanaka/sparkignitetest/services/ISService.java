package com.iamchanaka.sparkignitetest.services;

import com.iamchanaka.sparkignitetest.models.SampleData;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@Service
public class ISService {
    private final JavaIgniteContext<String, SampleData> igniteContext;
    private final SparkSession sparkSession;
    private final JavaSparkContext sparkContext;

    public ISService(JavaIgniteContext<String, SampleData> igniteContext, SparkSession sparkSession, JavaSparkContext sparkContext) {
        this.igniteContext = igniteContext;
        this.sparkSession = sparkSession;
        this.sparkContext = sparkContext;
    }

    public void loadData() throws AnalysisException {
        Dataset<Row> dataset = sparkSession.read()
                .option("header", "true")
                .parquet("C:\\Users\\chanakar\\Documents\\Projects\\Java\\SparkIgnitePrivate\\data\\die_t2.parquet");

        dataset.createTempView("SampleData");
    }

    public void full() throws AnalysisException {
        Dataset<Row> rawDataset = sparkSession.read()
                .option("header", "true")
                .parquet("C:\\Users\\chanakar\\Documents\\Projects\\Java\\SparkIgnitePrivate\\data\\die_t2.parquet");

        rawDataset.createTempView("SampleData");
        Dataset<Row> dataset = sparkSession.sql("select * from SampleData");
        Dataset<Row> selectedDataset = dataset
                .select("fab_die_x", "fab_die_y", "lg_key", "pg_key", "wf_key");
        List<Tuple2<String, SampleData>> data = new ArrayList<>();
        selectedDataset.foreach(row -> {
            Long x = row.<Long>getAs("fab_die_x");
            Long y = row.<Long>getAs("fab_die_y");
            Long lg = row.<Long>getAs("lg_key");
            Long pg = row.<Long>getAs("pg_key");
            Long wf = row.<Long>getAs("wf_key");
            SampleData sd = new SampleData(
                    x,
                    y,
                    lg,
                    pg,
                    wf
            );
            data.add(new Tuple2<>(x + "-" + y, sd));
        });

        JavaIgniteRDD<String, SampleData> igniteRDD = this.igniteContext.fromCache("default");
        JavaPairRDD<String, SampleData> pairRDD = this.sparkContext.parallelizePairs(data);
        igniteRDD.savePairs(pairRDD);
    }

    public void processData() {
        Dataset<Row> dataset = sparkSession.sql("select * from SampleData");
        Dataset<Row> selectedDataset = dataset
                .select("fab_die_x", "fab_die_y", "lg_key", "pg_key", "wf_key");
        List<Tuple2<String, SampleData>> data = new ArrayList<>();
        selectedDataset.foreach(row -> {
            Long x = row.<Long>getAs("fab_die_x");
            Long y = row.<Long>getAs("fab_die_y");
            Long lg = row.<Long>getAs("lg_key");
            Long pg = row.<Long>getAs("pg_key");
            Long wf = row.<Long>getAs("wf_key");
            SampleData sd = new SampleData(
                    x,
                    y,
                    lg,
                    pg,
                    wf
            );
            data.add(new Tuple2<>(x + "-" + y, sd));
        });

        JavaIgniteRDD<String, SampleData> igniteRDD = this.igniteContext.fromCache("default");
        JavaPairRDD<String, SampleData> pairRDD = this.sparkContext.parallelizePairs(data);
        igniteRDD.savePairs(pairRDD);
    }
}
