package org.tum.bpm.schemas.pmining;

import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Alarm {

    private String alarm;
    private String message;
    private Instant eventTime;
    private Instant alarmTime;


    public Alarm(String alarm, String message, Instant eventTime) {
        this.alarmTime = Instant.now();
        this.alarm = alarm;
        this.message = message;
        this.eventTime = eventTime;
    }
}
