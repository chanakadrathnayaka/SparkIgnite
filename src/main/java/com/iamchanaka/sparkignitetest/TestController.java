package com.iamchanaka.sparkignitetest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

  @Autowired
  private SparkService sparkService;

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
}
