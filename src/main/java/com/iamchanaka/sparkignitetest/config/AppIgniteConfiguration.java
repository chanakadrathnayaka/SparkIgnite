package com.iamchanaka.sparkignitetest.config;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppIgniteConfiguration {

  @Bean
  public Ignite igniteInstance() {
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

    return Ignition.start(cfg);
  }

  @Bean
  public IgniteClient igniteClient() {
    ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800"); // replace with your server's IP and port
    return Ignition.startClient(cfg);
  }
}
