package fuud.windowstat;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * This class counts measurements within time window. <br>
 * To do it efficiently it splits time window into chunks.<br>
 * Each chunk represent time period. <br>
 * Each chunk has expiration time. <br>
 * If current chunk is expired it is replaced with fresh one.<br>
 * Sum of measurements is accumulated and old chunk is removed.<br>
 * Let's consider chunks count = 5. Then sum in chunks 1-3 is accumulated and stored in chunk5 field. Thus current sum = chunk5 + chunk4 + (sum chunk1 + chunk2 + chunk3)<br>
 * But why we should not accumulate sum up to chunk 4? Because some threads can bypass expiration check but write measurement after chunk is expired and replaced:
 * <p>
 * <pre>
 *    |-----------------------------------------------------------------|
 *    | time   | thread 1               | thread 2                      |
 *    |-----------------------------------------------------------------|
 *    |  0     | chunk expiration time = 2                              |
 *    |-----------------------------------------------------------------|
 *    |  1     | isExpired() -> no      |                               |
 *    |  2     |                        | isExpired() -> yes            |
 *    |  3     |                        | replace chunk 1 with chunk 2  |
 *    |  4     | write data to chunk 1  |                               |
 *    |-----------------------------------------------------------------|
 * </pre>
 * <p>
 * If thread2 calculates all sum at time 3, it will lose measurement from thread1 writen.
 * This algorithm assumes that all writes to chunk1 is finished before chunk2 is expired.
 * With this assumption chunk should contains reference on previous chunk accumulator and calculated sum of previous chunks within time window except previous one.
 */

public class WindowMinMax {
    private final Duration windowSize;
    private final int chunkCount;
    private final Clock clock;
    private final long chunkDurationMs;

    private final AtomicReference<Chunk> currentChunk;

    public WindowMinMax(Duration windowSize, int chunkCount, Clock clock) {
        this.windowSize = windowSize;
        this.chunkCount = chunkCount;
        this.chunkDurationMs = windowSize.toMillis() / chunkCount;
        this.clock = clock;
        currentChunk = new AtomicReference<>(new Chunk(chunkCount, clock.millis() + chunkDurationMs));
    }

    public void register(long delta) {
        while (true) {
            final long currentTime = clock.millis();
            final Chunk currentChunk = this.currentChunk.get();
            if (!currentChunk.isExpired(currentTime)) {
                currentChunk.register(delta);
                return;
            }

            // current chunk isExpired, let's replace with new
            replaceChunkWithNew();
        }
    }

    public long getMax() {
        while (true) {
            final long currentTime = clock.millis();
            final Chunk currentChunk = this.currentChunk.get();
            if (!currentChunk.isExpired(currentTime)) {
                return currentChunk.getMax();
            }

            // current chunk isExpired, let's replace with new
            replaceChunkWithNew();
        }
    }

    public long getMin() {
        while (true) {
            final long currentTime = clock.millis();
            final Chunk currentChunk = this.currentChunk.get();
            if (!currentChunk.isExpired(currentTime)) {
                return currentChunk.getMin();
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

    private static class Chunk {
        private final long maxExceptThisAndPreviousChunk;
        private final long minExceptThisAndPreviousChunk;
        private final AtomicLong maxInThisChunk = new AtomicLong(Long.MIN_VALUE);
        private final AtomicLong minInThisChunk = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong maxInPrevChunk;
        private final AtomicLong minInPrevChunk;

        private final long[] maxInPrevChunks;
        private final long[] minInPrevChunks;

        private final long expirationTime;

        // if no previous chunk
        private Chunk(int chunkCount, long expirationTime) {
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
            this.expirationTime = expirationTime;

//            System.out.println("newChunk: " + this.toString());
        }

        public Chunk(long maxExceptThisAndPreviousChunk,
                     long minExceptThisAndPreviousChunk,
                     AtomicLong maxInPrevChunk,
                     AtomicLong minInPrevChunk,
                     long[] maxInPrevChunks,
                     long[] minInPrevChunks,
                     long expirationTime) {
            this.maxExceptThisAndPreviousChunk = maxExceptThisAndPreviousChunk;
            this.minExceptThisAndPreviousChunk = minExceptThisAndPreviousChunk;
            this.maxInPrevChunk = maxInPrevChunk;
            this.minInPrevChunk = minInPrevChunk;
            this.maxInPrevChunks = maxInPrevChunks;
            this.minInPrevChunks = minInPrevChunks;
            this.expirationTime = expirationTime;
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

        private void register(long value) {
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
        }

        private boolean isExpired(long currentTime) {
            return expirationTime <= currentTime;
        }

        private Chunk createNext(long chunkDurationMs) {
            final long maxInPrevChunk = getMaxInPrevChunk();
            final long minInPrevChunk = getMinInPrevChunk();

            long newMaxExceptThisAndPreviousChunk = Long.MIN_VALUE;
            long newMinExceptThisAndPreviousChunk = Long.MAX_VALUE;
            
            for (int i=1; i<maxInPrevChunks.length; i++){
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

            final Chunk chunk = new Chunk(
                    newMaxExceptThisAndPreviousChunk,
                    newMinExceptThisAndPreviousChunk,
                    maxInThisChunk,
                    minInThisChunk,
                    newMaxInPrevChunks,
                    newMinInPrevChunks,
                    expirationTime + chunkDurationMs);
            return chunk;
        }

        private long getMaxInPrevChunk() {
            return maxInPrevChunk.longValue();
        }

        private long getMinInPrevChunk() {
            return minInPrevChunk.longValue();
        }


        @Override
        public String toString() {
            return "Chunk{" +
                    "maxExceptThisAndPreviousChunk=" + maxExceptThisAndPreviousChunk +
                    ", minExceptThisAndPreviousChunk=" + minExceptThisAndPreviousChunk +
                    ", maxInThisChunk=" + maxInThisChunk +
                    ", minInThisChunk=" + minInThisChunk +
                    ", maxInPrevChunk=" + maxInPrevChunk +
                    ", minInPrevChunk=" + minInPrevChunk +
                    ", maxInPrevChunks=" + Arrays.toString(maxInPrevChunks) +
                    ", minInPrevChunks=" + Arrays.toString(minInPrevChunks) +
                    ", expirationTime=" + expirationTime +
                    '}';
        }
    }
}
