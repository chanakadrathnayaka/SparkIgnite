package com.iamchanaka.sparkignitetest.config;

import com.iamchanaka.sparkignitetest.models.SampleData;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.IgniteContext;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.Serializable;
import java.util.Collections;

@Configuration
public class SparkIgniteConfig implements Serializable {

    private final SparkContext sparkContext;

    public SparkIgniteConfig(SparkSession sparkSession) {
        this.sparkContext = sparkSession.sparkContext();
    }

    public IgniteContext createIgniteContext() {
        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        igniteConfiguration.setClientMode(true);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
        spi.setIpFinder(ipFinder);
        igniteConfiguration.setDiscoverySpi(spi);

        CacheConfiguration<Integer, String> cacheConfiguration = new CacheConfiguration<>("default");
        cacheConfiguration.setBackups(1);
        cacheConfiguration.setName("default");
        cacheConfiguration.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);

        return new IgniteContext(this.sparkContext, () -> igniteConfiguration, true);
    }

    @Bean
    public JavaSparkContext getSparkContext() {
        return new JavaSparkContext(this.sparkContext);
    }

    @Bean
    public JavaIgniteContext<String, SampleData> createJavaIgniteContext() {

        JavaSparkContext sparkContext = new JavaSparkContext(this.sparkContext);


        return new JavaIgniteContext<>(sparkContext,
                () -> {
                    IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
                    igniteConfiguration.setClientMode(true);

                    TcpDiscoverySpi spi = new TcpDiscoverySpi();
                    TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
                    ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
                    spi.setIpFinder(ipFinder);
                    igniteConfiguration.setDiscoverySpi(spi);

                    CacheConfiguration<String, SampleData> cacheConfiguration = new CacheConfiguration<>("default");
                    cacheConfiguration.setBackups(1);
                    cacheConfiguration.setName("default");
                    cacheConfiguration.setAtomicityMode(CacheAtomicityMode.ATOMIC);
                    igniteConfiguration.setCacheConfiguration(cacheConfiguration);

                    return igniteConfiguration;
                }, true);
    }
}
