package org.tum.bpm.schemas.ocel;

import java.time.Instant;
import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OcelEvent implements Comparable<OcelEvent> {
    private String id;
    private String type;
    private Instant time;
    private List<OcelAttribute> attributes;
    private List<OcelRelationship> relationships;

    @Override
    public int compareTo(OcelEvent other) {
        return this.time.compareTo(other.getTime());
    }
}
