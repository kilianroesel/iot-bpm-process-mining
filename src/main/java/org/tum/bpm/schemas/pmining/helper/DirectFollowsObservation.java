package org.tum.bpm.schemas.pmining.helper;

import java.time.Duration;

import lombok.Getter;

@Getter
public class DirectFollowsObservation extends Observation {

    private Duration averageDuration;

    public DirectFollowsObservation(long error, Duration firstDuration) {
        super(error);
        this.averageDuration = firstDuration;
    }

    /**
     * Call after incrementing the count
     * @param newDuration
     */
    public void addDuration(Duration newDuration) {
        this.averageDuration = this.averageDuration.multipliedBy(this.count - 1)
                .plus(newDuration)
                .dividedBy(this.count);
    }
}
