package fuud.windowstat;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;

public class CounterWindowHistogram implements WindowHistogram {
    private final long[] bucketOffsets;
    private final WindowCounter[] buckets;
    private final WindowMinMax minMax;
    private final WindowCounter total;
    private final WindowCounter samplesCount;

    /**
     * Let's bucketOffsets = [1, 2, 10] <br>
     * Histogram buckets:
     * <pre>
     *   |Long.MIN_VALUE  |   1             | 2 | 3 | ... | 9 | 10 |  Long.MAX_VALUE |
     *   <-- bucket[0] ---><-- bucket[1] ---><---- bucket[2] ------><---- bucket[3] ->
     *
     *   bucket[0] - from Long.MIN_VALUE inclusive to bucketOffsets[0] exclusive
     *   bucket[1] - bucketOffsets[0] inclusive to bucketOffsets[1] exclusive
     *   bucket[2] - bucketOffsets[1] inclusive to bucketOffsets[2] exclusive
     *   bucket[3] - bucketOffsets[2] inclusive to Long.MAX_VALUE inclusive
     * </pre>
     * <p>
     * If bucket[0] contain values => histogram is underflow. <br>
     * If last bucket contain values => histogram is overflow.
     */
    public CounterWindowHistogram(long[] bucketOffsets, Duration windowSize, int chunkCount, Clock clock) {

        for (int i = 1; i < bucketOffsets.length; i++) {
            if (bucketOffsets[i - 1] >= bucketOffsets[i]) {
                throw new IllegalArgumentException("Bucket offsets should be monotonically increasing sequence");
            }
        }

        this.bucketOffsets = bucketOffsets;
        buckets = new WindowCounter[bucketOffsets.length + 1];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new WindowCounter(windowSize, chunkCount, clock);
        }

        minMax = new WindowMinMax(windowSize, chunkCount, clock);
        total = new WindowCounter(windowSize, chunkCount, clock);
        samplesCount = new WindowCounter(windowSize, chunkCount, clock);
    }

    @Override
    public long getMax() {
        return minMax.getMax();
    }

    @Override
    public long getMin() {
        return minMax.getMin();
    }

    @Override
    public long getMean() {
        final long samplesCount = this.samplesCount.getSum();
        if (samplesCount == 0) {
            return 0;
        } else {
            return total.getSum() / samplesCount;
        }
    }

    @Override
    public long getPercentile(double percentile) {
        long count = 0;
        for (WindowCounter bucket : buckets) {
            count += bucket.getSum();
        }

        if (count == 0) {
            return 0;
        } else {
            int countUnderPercentile = (int) (count * percentile); // math round
            if (countUnderPercentile == 0) {
                countUnderPercentile = 1;
            }
            count = 0;
            for (int i = 0; i < buckets.length; i++) {
                count += buckets[i].getSum();
                if (count >= countUnderPercentile) {
                    int bucketOffsetIndex = i - 1; // bucket offsets does not include "underflow" bucket
                    if (bucketOffsetIndex < 0) {
                        return Long.MIN_VALUE; // can not calc
                    }
                    if (bucketOffsetIndex >= bucketOffsets.length - 1) {
                        return Long.MAX_VALUE; // overflow
                    }
                    return bucketOffsets[bucketOffsetIndex];
                }
            }
            throw new IllegalStateException("Should not reach here");
        }
    }

    @Override
    public boolean isOverflow() {
        return buckets[buckets.length - 1].getSum() > 0;
    }

    @Override
    public boolean isUnderflow() {
        return buckets[0].getSum() > 0;
    }

    @Override
    public Bucket[] getSnapshot() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSamplesCount() {
        return samplesCount.getSum();
    }

    @Override
    public void add(long value) {
        final int bucket = Arrays.binarySearch(bucketOffsets, value);
        if (bucket >= 0) {
            buckets[bucket + 1].add(1);
        } else {
            buckets[-bucket - 1].add(1);
        }

        minMax.register(value);
        total.add(value);
        samplesCount.add(1);
    }

    public class Bucket {
        private final long minValueInclusive;
        private final long maxValueInclusive;
        private final long count;

        public Bucket(long minValueInclusive, long maxValueInclusive, long count) {
            this.minValueInclusive = minValueInclusive;
            this.maxValueInclusive = maxValueInclusive;
            this.count = count;
        }

        public long getMinValueInclusive() {
            return minValueInclusive;
        }

        public long getMaxValueInclusive() {
            return maxValueInclusive;
        }

        public long getCount() {
            return count;
        }
    }
}
