package fuud.windowstat;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.LongStream;

public class CompactWindowHistogram implements WindowHistogram {

    private final Clock clock;
    private final long chunkDurationMs;
    private final AtomicReference<Chunk> currentChunk;

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
    public CompactWindowHistogram(long[] bucketOffsets, Duration windowSize, int chunkCount, Clock clock) {
        this.clock = clock;

        for (int i = 1; i < bucketOffsets.length; i++) {
            if (bucketOffsets[i - 1] >= bucketOffsets[i]) {
                throw new IllegalArgumentException("Bucket offsets should be monotonically increasing sequence");
            }
        }

        chunkDurationMs = windowSize.toMillis() / chunkCount;
        currentChunk = new AtomicReference<>(new Chunk(bucketOffsets, chunkCount, clock.millis() + chunkDurationMs));
    }

    private Chunk getActualChunk() {
        while (true) {
            final long currentTime = clock.millis();
            final Chunk currentChunk = this.currentChunk.get();
            if (!currentChunk.isExpired(currentTime)) {
                return currentChunk;
            }

            // current chunk isExpired, let's replace with new
            replaceChunkWithNew();
        }
    }

    private void replaceChunkWithNew() {
        final long currentTime = clock.millis();

        while (true) {
            final Chunk currentChunk = this.currentChunk.get();
            if (!currentChunk.isExpired(currentTime)) {
                break;
            } else {
                Chunk newChunk = currentChunk.createNext(chunkDurationMs);
                this.currentChunk.compareAndSet(currentChunk, newChunk);
            }
        }
    }

    @Override
    public void add(long value) {
        getActualChunk().add(value);
    }

    @Override
    public long getMax() {
        return getActualChunk().getSnapshot().getMax();
    }

    @Override
    public long getMin() {
        return getActualChunk().getSnapshot().getMin();
    }

    @Override
    public long getMean() {
        return getActualChunk().getSnapshot().getMean();
    }

    @Override
    public long getPercentile(double percentile) {
        return getActualChunk().getSnapshot().getPercentile(percentile);
    }

    @Override
    public boolean isOverflow() {
        return getActualChunk().getSnapshot().isOverflow();
    }

    @Override
    public boolean isUnderflow() {
        return getActualChunk().getSnapshot().isUnderflow();
    }

    @Override
    public CounterWindowHistogram.Bucket[] getSnapshot() {
        return new CounterWindowHistogram.Bucket[0];
    }

    @Override
    public long getSamplesCount() {
        return getActualChunk().getSnapshot().getSamplesCount();
    }

    private static class Chunk {
        private final int bucketsCount;
        private final long[] accumulatedSumExceptThisAndPreviousChunk;
        private final AtomicLongArray sumInPrevChunk;
        private final AtomicLongArray sumInThisChunk;
        /**
         * byChunkPrevSums[0] - contains value that was in previous to the last chunk in window - needed to smoothing
         * byChunkPrevSums[1] - contains value that was in last chunk in window
         * byChunkPrevSums[2] - contains value that was in the next to the last chunk in window
         * byChunkPrevSums[3] - contains value of chunks in the next to the next to the last chunk in window
         * etc up to previous chunk (exclusive). Previous value can be found in sumInPrevChunk field
         */
        private final long[][] sumInPrevChunks;

        // --- min-max
        private final long[] bucketOffsets;
        private final long maxExceptThisAndPreviousChunk;
        private final long minExceptThisAndPreviousChunk;
        private final AtomicLong maxInThisChunk = new AtomicLong(Long.MIN_VALUE);
        private final AtomicLong minInThisChunk = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong maxInPrevChunk;
        private final AtomicLong minInPrevChunk;

        private final long[] maxInPrevChunks;
        private final long[] minInPrevChunks;

        // for mean - total sum
        private final long totalAccumulatedSumExceptThisAndPreviousChunk;
        private final LongAdder totalSumInPrevChunk;
        private final LongAdder totalSumInThisChunk = new LongAdder();
        private final long[] totalSumInPrevChunks;

        private final long expirationTime;

        // if no previous chunk
        private Chunk(long[] bucketOffsets, int chunkCount, long expirationTime) {
            this.bucketOffsets = bucketOffsets;
            bucketsCount = bucketOffsets.length + 1;
            this.accumulatedSumExceptThisAndPreviousChunk = new long[bucketsCount];
            this.sumInPrevChunk = new AtomicLongArray(bucketsCount);
            this.sumInThisChunk = new AtomicLongArray(bucketsCount);
            this.sumInPrevChunks = new long[chunkCount - 1 - 1 + 1][bucketsCount]; // except this, except previous, include previous to the last

            // min-max
            this.maxExceptThisAndPreviousChunk = Long.MIN_VALUE;
            this.minExceptThisAndPreviousChunk = Long.MAX_VALUE;

            this.maxInPrevChunk = new AtomicLong(Long.MIN_VALUE);
            this.minInPrevChunk = new AtomicLong(Long.MAX_VALUE);

            this.maxInPrevChunks = new long[chunkCount - 1 - 1]; // except this, except previous
            for (int i = 0; i < maxInPrevChunks.length; i++) {
                maxInPrevChunks[i] = Long.MIN_VALUE;
            }
            this.minInPrevChunks = new long[chunkCount - 1 - 1]; // except this, except previous
            for (int i = 0; i < minInPrevChunks.length; i++) {
                minInPrevChunks[i] = Long.MAX_VALUE;
            }

            //total
            this.totalAccumulatedSumExceptThisAndPreviousChunk = 0;
            this.totalSumInPrevChunk = new LongAdder();
            this.totalSumInPrevChunks = new long[chunkCount - 1 - 1 + 1];


            this.expirationTime = expirationTime;

//            System.out.println("newChunk: " + this.toString());
        }

        private Chunk(long[] bucketOffsets,
                      long[] accumulatedSumExceptThisAndPreviousChunk,
                      AtomicLongArray sumInPrevChunk,
                      long[][] sumInPrevChunks,

                      long maxExceptThisAndPreviousChunk,
                      long minExceptThisAndPreviousChunk,
                      AtomicLong maxInPrevChunk,
                      AtomicLong minInPrevChunk,
                      long[] maxInPrevChunks,
                      long[] minInPrevChunks,

                      long totalAccumulatedSumExceptThisAndPreviousChunk,
                      LongAdder totalSumInPrevChunk,
                      long[] totalSumInPrevChunks,

                      long expirationTime) {
            this.bucketOffsets = bucketOffsets;
            bucketsCount = bucketOffsets.length + 1;
            this.accumulatedSumExceptThisAndPreviousChunk = accumulatedSumExceptThisAndPreviousChunk;
            this.sumInPrevChunk = sumInPrevChunk;
            this.sumInThisChunk = new AtomicLongArray(bucketsCount);
            this.sumInPrevChunks = sumInPrevChunks;

            this.maxExceptThisAndPreviousChunk = maxExceptThisAndPreviousChunk;
            this.minExceptThisAndPreviousChunk = minExceptThisAndPreviousChunk;
            this.maxInPrevChunk = maxInPrevChunk;
            this.minInPrevChunk = minInPrevChunk;
            this.maxInPrevChunks = maxInPrevChunks;
            this.minInPrevChunks = minInPrevChunks;

            this.totalAccumulatedSumExceptThisAndPreviousChunk = totalAccumulatedSumExceptThisAndPreviousChunk;
            this.totalSumInPrevChunk = totalSumInPrevChunk;
            this.totalSumInPrevChunks = totalSumInPrevChunks;


            this.expirationTime = expirationTime;

//            System.out.println("newChunk: " + this.toString());
        }

        private Chunk createNext(long chunkDurationMs) {
            final long[] sumInPrevChunk = getSumInPrevChunk();
            final long[] sumInLastChunkInWindow = getSumInLastChunkInWindow();

            final long[] newAccumulatedSumExceptThisAndPreviousChunk = new long[bucketsCount];
            for (int i = 0; i < bucketsCount; i++) {
                newAccumulatedSumExceptThisAndPreviousChunk[i] =
                        accumulatedSumExceptThisAndPreviousChunk[i]
                                - sumInLastChunkInWindow[i]
                                + sumInPrevChunk[i];
            }

            long[][] newSumInPrevChunks = new long[sumInPrevChunks.length][bucketsCount];
            System.arraycopy(sumInPrevChunks, 1, newSumInPrevChunks, 0, newSumInPrevChunks.length - 1); // shift, add previous to sums
            newSumInPrevChunks[newSumInPrevChunks.length - 1] = sumInPrevChunk;

            // min-max
            final long maxInPrevChunk = getMaxInPrevChunk();
            final long minInPrevChunk = getMinInPrevChunk();

            long newMaxExceptThisAndPreviousChunk = Long.MIN_VALUE;
            long newMinExceptThisAndPreviousChunk = Long.MAX_VALUE;

            for (int i = 1; i < maxInPrevChunks.length; i++) {
                newMaxExceptThisAndPreviousChunk = Math.max(newMaxExceptThisAndPreviousChunk, maxInPrevChunks[i]);
                newMinExceptThisAndPreviousChunk = Math.min(newMinExceptThisAndPreviousChunk, minInPrevChunks[i]);
            }

            newMaxExceptThisAndPreviousChunk = Math.max(newMaxExceptThisAndPreviousChunk, maxInPrevChunk);
            newMinExceptThisAndPreviousChunk = Math.min(newMinExceptThisAndPreviousChunk, minInPrevChunk);

            long[] newMaxInPrevChunks = new long[maxInPrevChunks.length];
            System.arraycopy(maxInPrevChunks, 1, newMaxInPrevChunks, 0, newMaxInPrevChunks.length - 1); // shift, add previous to sums
            newMaxInPrevChunks[newMaxInPrevChunks.length - 1] = maxInPrevChunk;

            long[] newMinInPrevChunks = new long[minInPrevChunks.length];
            System.arraycopy(minInPrevChunks, 1, newMinInPrevChunks, 0, newMinInPrevChunks.length - 1); // shift, add previous to sums
            newMinInPrevChunks[newMinInPrevChunks.length - 1] = minInPrevChunk;

            //total
            final long totalSumInPrevChunk = this.totalSumInPrevChunk.longValue();

            final long newTotalAccumulatedSumExceptThisAndPreviousChunk =
                    totalAccumulatedSumExceptThisAndPreviousChunk
                            - getTotalSumInLastChunkInWindow()
                            + totalSumInPrevChunk;

            long[] newTotalSumInPrevChunks = new long[sumInPrevChunks.length];
            System.arraycopy(sumInPrevChunks, 1, newSumInPrevChunks, 0, newSumInPrevChunks.length - 1); // shift, add previous to sums
            newSumInPrevChunks[newSumInPrevChunks.length - 1] = sumInPrevChunk;

            final Chunk chunk = new Chunk(

                    bucketOffsets,
                    newAccumulatedSumExceptThisAndPreviousChunk,
                    sumInThisChunk,
                    newSumInPrevChunks,

                    newMaxExceptThisAndPreviousChunk,
                    newMinExceptThisAndPreviousChunk,
                    maxInThisChunk,
                    minInThisChunk,
                    newMaxInPrevChunks,
                    newMinInPrevChunks,

                    newTotalAccumulatedSumExceptThisAndPreviousChunk,
                    totalSumInThisChunk,
                    newTotalSumInPrevChunks,

                    expirationTime + chunkDurationMs
            );
            return chunk;
        }

        private long getMaxInPrevChunk() {
            return maxInPrevChunk.longValue();
        }

        private long getMinInPrevChunk() {
            return minInPrevChunk.longValue();
        }

        private void add(long value) {
            final int bucket = Arrays.binarySearch(bucketOffsets, value);
            if (bucket >= 0) {
                sumInThisChunk.incrementAndGet(bucket + 1);
            } else {
                sumInThisChunk.incrementAndGet(-bucket - 1);
            }

            while (true) {
                final long currentMaxValue = maxInThisChunk.get();
                if (currentMaxValue > value) {
                    break;
                }
                if (maxInThisChunk.compareAndSet(currentMaxValue, value)) {
                    break;
                }
            }

            while (true) {
                final long currentMinValue = minInThisChunk.get();
                if (currentMinValue < value) {
                    break;
                }
                if (minInThisChunk.compareAndSet(currentMinValue, value)) {
                    break;
                }
            }

            totalSumInThisChunk.add(value);

        }

        private Snapshot getSnapshot() {

            long[] bucketValues = new long[bucketsCount];

            for (int i = 0; i < bucketsCount; i++) {
                bucketValues[i] =
                        accumulatedSumExceptThisAndPreviousChunk[i] +
                                sumInPrevChunk.get(i) +
                                sumInThisChunk.get(i);
            }

            return new Snapshot(
                    getMin(),
                    getMax(),
                    bucketOffsets,
                    bucketValues,
                    getTotalSum()
            );
        }

        private boolean isExpired(long currentTime) {
            return expirationTime <= currentTime;
        }

        private long[] getSumInPrevChunk() {
            long[] result = new long[sumInPrevChunk.length()];
            for (int i = 0; i < result.length; i++) {
                result[i] = sumInPrevChunk.get(i);
            }
            return result;
        }

        private long[] getSumInLastChunkInWindow() {
            return sumInPrevChunks[1];
        }

        private long getTotalSumInLastChunkInWindow() {
            return totalSumInPrevChunks[1];
        }

        private long getMin() {
            final long min = Math.min(
                    Math.min(minExceptThisAndPreviousChunk, minInPrevChunk.get()),
                    minInThisChunk.get()
            );
            return min == Long.MAX_VALUE ? 0 : min;
        }

        private long getMax() {
            final long max = Math.max(
                    Math.max(maxExceptThisAndPreviousChunk, maxInPrevChunk.get()),
                    maxInThisChunk.get()
            );
            return max == Long.MIN_VALUE ? 0 : max;
        }

        private long getTotalSum(){
            return totalAccumulatedSumExceptThisAndPreviousChunk + totalSumInPrevChunk.longValue() + totalSumInThisChunk.longValue();
        }
    }
}
