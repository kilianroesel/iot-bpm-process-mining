package org.tum.bpm.schemas.pmining;

import java.time.Instant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.tum.bpm.schemas.ocel.OcelEvent;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeviceOcelEvent {

    @JsonProperty("document")
    private OcelEvent event;
    private String device;
    private Instant eventTime;
    private Instant sendTime;
    private Instant ingestionTime;
    private Instant sinkTime;
    private Instant scopeTime;
    private Instant abstractionTime;
    private Instant enrichmentTime;
    private Instant correlationTime;

    public DeviceOcelEvent(OcelEvent event, String device) {
        this.event = event;
        this.device = device;
    }
}
