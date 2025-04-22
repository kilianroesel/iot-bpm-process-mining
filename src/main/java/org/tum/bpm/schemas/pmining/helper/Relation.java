package org.tum.bpm.schemas.pmining.helper;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class Relation {
    
    private String source;
    private String target;
}
