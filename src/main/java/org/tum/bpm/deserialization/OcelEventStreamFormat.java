package org.tum.bpm.deserialization;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.tum.bpm.schemas.ocel.OcelEvent;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class OcelEventStreamFormat extends SimpleStreamFormat<OcelEvent> {

    public static final String DEFAULT_CHARSET_NAME = "UTF-8";

    private final String charsetName;

    public OcelEventStreamFormat() {
        this(DEFAULT_CHARSET_NAME);
    }

    public OcelEventStreamFormat(String charsetName) {
        this.charsetName = charsetName;
    }

    @Override
    public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(stream, charsetName));
        return new Reader(reader);
    }

    @Override
    public TypeInformation<OcelEvent> getProducedType() {
        // TODO Auto-generated method stub
        return TypeInformation.of(new TypeHint<OcelEvent>() {});
    }

        /** The actual reader for the {@code TextLineInputFormat}. */
    @PublicEvolving
    public static final class Reader implements StreamFormat.Reader<OcelEvent> {

        private final BufferedReader reader;
        private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        private Iterator<OcelEvent> currentIterator;

        Reader(final BufferedReader reader) {
            this.reader = reader;
        }

        @Nullable
        @Override
        public OcelEvent read() throws IOException {
            if (currentIterator == null) {
                // Read and parse the next list of events
                List<OcelEvent> events = this.objectMapper.readValue(this.reader, new TypeReference<List<OcelEvent>>() {});
                currentIterator = events.iterator();
            }
            
            return currentIterator != null && currentIterator.hasNext() ? currentIterator.next() : null;
        }

        @Override
        public void close() throws IOException {
            System.out.println("closed");
            reader.close();
        }
    }
}