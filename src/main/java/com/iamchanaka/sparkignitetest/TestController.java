package com.iamchanaka.sparkignitetest;

import com.iamchanaka.sparkignitetest.services.IgniteService;
import com.iamchanaka.sparkignitetest.services.SparkService;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

  @Autowired
  private SparkService sparkService;
  @Autowired
  private IgniteService igniteService;

  @GetMapping("index")
  public String index() {
    return "indexx";
  }

  @GetMapping("create")
  public void createData() {
    this.sparkService.createDataSet();
  }

  @GetMapping("read")
  public void readData() {
    this.sparkService.readDataSet();
  }

  @GetMapping("write-thick")
  public void writeThickData() {
    this.igniteService.writeThickClientMode();
  }

  @GetMapping("read-thick")
  public Map<Integer, String> readThickData() {
    return this.igniteService.readThickClientMode();
  }
  @GetMapping("write-thin")
  public void writeThinData() {
    this.igniteService.writeThinClientMode();
  }

  @GetMapping("read-thin")
  public Map<Integer, String> readThinData() {
    return this.igniteService.readThinClientMode();
  }
}
