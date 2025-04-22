package org.tum.bpm.schemas.pmining.helper;

import java.time.Instant;

import lombok.Getter;

@Getter
public class CaseObservation extends Observation {
    
    private String latestActivity;
    private Instant latestActivityTime;
    private String secondLatestActivity;

    public CaseObservation(String lastActivity, Instant latestActivityTime, long error) {
        super(error);
        this.latestActivity = lastActivity;
        this.latestActivityTime = latestActivityTime;
    }

    public void setLatestActivity(String lastActivity, Instant latestActivityTime) {
        this.secondLatestActivity = this.latestActivity;
        this.latestActivity = lastActivity;

        this.latestActivityTime = latestActivityTime;
    }
}
