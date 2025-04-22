package org.tum.bpm.schemas.pmining.helper;

import lombok.Getter;

@Getter
public class Observation {

    protected long count;
    protected long error;

    public Observation(long error) {
        this.count = 1L;
        this.error = error;
    }

    public void incrementCount() {
        this.count++;
    }
}
