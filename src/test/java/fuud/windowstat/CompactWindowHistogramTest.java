package fuud.windowstat;

import fuud.windowstat.util.MockClock;

import java.time.Duration;

public class CompactWindowHistogramTest extends WindowHistogramTest {

    protected WindowHistogram createHistogram(long[] bucketOffsets, Duration windowSize, int chunkCount, MockClock clock) {
        return new CompactWindowHistogram(bucketOffsets, windowSize, chunkCount, clock);
    }
}
