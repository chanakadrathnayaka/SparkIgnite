package com.iamchanaka.sparkignitetest;

import com.iamchanaka.sparkignitetest.services.ISService;
import com.iamchanaka.sparkignitetest.services.SparkService;
import org.apache.spark.sql.AnalysisException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @Autowired
    private SparkService sparkService;
    @Autowired
    private ISService igniteSparkService;

    @GetMapping("index")
    public String index() {
        return "index";
    }

    @GetMapping("create")
    public void createData() {
        this.sparkService.createDataSet();
    }

    @GetMapping("read")
    public void readData() {
        this.sparkService.readDataSet();
    }

    @GetMapping("load")
    public String loadData() throws AnalysisException {
        this.igniteSparkService.loadData();
        return "Loaded";
    }

    @GetMapping("full")
    public String fullData() throws AnalysisException {
        this.igniteSparkService.full();
        return "Loaded";
    }

    @GetMapping("process")
    public String processData() {
        this.igniteSparkService.processData();

        return "Processed";
    }
}
