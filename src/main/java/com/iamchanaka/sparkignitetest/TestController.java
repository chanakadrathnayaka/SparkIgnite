package com.iamchanaka.sparkignitetest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

  @GetMapping("index")
  public String index() {
    return "indexx";
  }
}
