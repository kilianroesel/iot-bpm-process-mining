package org.tum.bpm.schemas.pmining;

import java.io.Serializable;
import java.time.Instant;

import lombok.Data;

@Data
public abstract class ProcessMiningResponse implements Serializable {

	private Instant startTime;
	private Instant endTime;
	private long processedEvents;
	private String objectView;
	private String device;
}
