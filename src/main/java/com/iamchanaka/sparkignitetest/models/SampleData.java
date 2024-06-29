package com.iamchanaka.sparkignitetest.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
public class SampleData implements Serializable {

    private Long fabDieX;
    private Long fabDieY;
    private Long lgKey;
    private Long pgKey;
    private Long wfKey;
}
