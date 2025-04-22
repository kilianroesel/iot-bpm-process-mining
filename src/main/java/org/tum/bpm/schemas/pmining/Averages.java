package org.tum.bpm.schemas.pmining;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Averages {
    double avgUpstream;
    double avgScoping;
    double avgAbstraction;
    double avgEnrichment;
    double avgCorrelation;
    double avgSink;
    double totalSum;
    double count;
}