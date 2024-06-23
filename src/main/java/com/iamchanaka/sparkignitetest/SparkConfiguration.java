package com.iamchanaka.sparkignitetest;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class SparkConfiguration {

  @Value("${spring.application.name}")
  private String appName;

  @Value("${spark.master}")
  private String masterUri;

  @Bean()
  public SparkSession createSparkSession() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName(this.appName);
    sparkConf.setMaster(this.masterUri);

    return SparkSession.builder().config(sparkConf).getOrCreate();
  }
}
