package org.tum.bpm.sources;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.tum.bpm.deserialization.OcelEventStreamFormat;
import org.tum.bpm.schemas.ocel.OcelEvent;

public class OcelFileSource {


    public static FileSource<OcelEvent> createOcelEventSource() throws IOException {
        File file = new File("sorted_output.json");
        if (!file.exists()) {
            throw new IOException("File not found: ");
        }
        FileSource<OcelEvent> source = FileSource
                .forRecordStreamFormat(new OcelEventStreamFormat(), Path.fromLocalFile(file)).build();

        return source;
    }

    public static WatermarkStrategy<OcelEvent> createWatermarkStrategy() {
        WatermarkStrategy<OcelEvent> watermarkStrategy = WatermarkStrategy
                // Handle data that is max 20 seconds out of order
                .<OcelEvent>forBoundedOutOfOrderness(Duration.ofDays(750))
                // Assign event Time Timestamps to each measurement
                .withTimestampAssigner((event, timestamp) -> event.getTime().toEpochMilli())
                // If a source does not generate events for one hour it is considered idle and
                // the watermark progresses
                .withIdleness(Duration.ofHours(1));
        return watermarkStrategy;
    }
}
