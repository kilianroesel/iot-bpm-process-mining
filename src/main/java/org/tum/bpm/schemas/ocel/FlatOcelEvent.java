package org.tum.bpm.schemas.ocel;

import java.time.Instant;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
/**
 * A Ocel event but it has been flattened to one relationship, a.k. process case
 */
public class FlatOcelEvent implements Comparable<FlatOcelEvent> {
    private String id;
    private String type;
    private Instant time;
    private OcelRelationship processCase;
    private List<OcelAttribute> attributes;
    private String device;

    @Override
    public int compareTo(FlatOcelEvent other) {
        return this.time.compareTo(other.getTime());
    }
}
