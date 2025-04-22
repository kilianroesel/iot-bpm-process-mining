package org.tum.bpm.schemas.ocel;

import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OcelObject {
    private String id;
    private String type;
    private List<OcelAttribute> attributes;
    private List<OcelRelationship> relationships;
}
