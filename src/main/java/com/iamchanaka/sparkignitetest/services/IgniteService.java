package com.iamchanaka.sparkignitetest.services;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.springframework.stereotype.Service;

@Service
public class IgniteService {

  private final Ignite ignite;
  private final IgniteClient igniteClient;

  public IgniteService(Ignite ignite, IgniteClient igniteClient) {
    this.ignite = ignite;
    this.igniteClient = igniteClient;
  }

  public void writeThickClientMode() {
    IgniteCache<Integer, String> cache = ignite.cache("default");
    cache.put(1, "Chanaka");
    cache.put(2, "Nadeera");
  }

  public Map<Integer, String> readThickClientMode() {
    IgniteCache<Integer, String> cache = ignite.cache("default");

    Map<Integer, String> data = new HashMap<>();

    cache.forEach(entry -> {
      data.put(entry.getKey(), entry.getValue());
    });

    return data;
  }

  public void writeThinClientMode() {
    ClientCache<Integer, String> cache = igniteClient.cache("default");
    cache.put(3, "Chanaka New");
    cache.put(4, "Nadeera New");
  }

  public Map<Integer, String> readThinClientMode() {
    ClientCache<Integer, String> cache = igniteClient.cache("default");
    cache.get(3);
    cache.get(4);

    Map<Integer, String> data = new HashMap<>();

    data.put(3, cache.get(3));
    data.put(4, cache.get(4));

    return data;
  }
}
