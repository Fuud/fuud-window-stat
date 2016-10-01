package fuud.windowstat;

import fuud.windowstat.util.MockClock;

import java.time.Duration;

public class CounterWindowHistogramTest extends WindowHistogramTest {

    protected WindowHistogram createHistogram(long[] bucketOffsets, Duration windowSize, int chunkCount, MockClock clock) {
        return new CounterWindowHistogram(bucketOffsets, windowSize, chunkCount, clock);
    }
}
