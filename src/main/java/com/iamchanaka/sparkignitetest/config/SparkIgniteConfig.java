package com.iamchanaka.sparkignitetest.config;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkIgniteConfig {

  @Value("${spring.application.name}")
  private String appName;

  @Value("${spark.master}")
  private String masterUri;

  @Bean
  public Ignite config() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName("SparkIgniteApp");
    sparkConf.setMaster(this.masterUri);

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    // Ignite configuration
//    IgniteConfiguration igniteCfg = new IgniteConfiguration();
//    igniteCfg.setClientMode(true); // Use client mode for Spark integration
    IgniteConfiguration cfg = new IgniteConfiguration();

    // Setting client mode
    cfg.setClientMode(true);

    TcpDiscoverySpi spi = new TcpDiscoverySpi();
    TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
    ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
    spi.setIpFinder(ipFinder);
    cfg.setDiscoverySpi(spi);

    CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>("default");
    ccfg.setBackups(1);
    ccfg.setName("default");
    ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
    cfg.setCacheConfiguration(ccfg);

    // Create Ignite context
    JavaIgniteContext<Integer, String> igniteContext = new JavaIgniteContext<>(sparkContext,
        () -> cfg);
    return igniteContext.ignite();
  }
}
