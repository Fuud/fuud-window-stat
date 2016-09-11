package fuud.windowstat;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;


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

public class WindowCounter {
    private final Duration windowSize;
    private final int chunkCount;
    private final Clock clock;
    private final long chunkDurationMs;

    private AtomicReference<Chunk> currentChunk;

    public WindowCounter(Duration windowSize, int chunkCount, Clock clock) {
        this.windowSize = windowSize;
        this.chunkCount = chunkCount;
        this.chunkDurationMs = windowSize.toMillis() / chunkCount;
        this.clock = clock;
        currentChunk = new AtomicReference<>(new Chunk(chunkCount, clock.millis() + chunkDurationMs));
    }

    public void add(long delta) {
        while (true) {
            final long currentTime = clock.millis();
            final Chunk currentChunk = this.currentChunk.get();
            if (!currentChunk.isExpired(currentTime)) {
                currentChunk.add(delta);
                return;
            }

            // current chunk isExpired, let's replace with new
            replaceChunkWithNew();
        }
    }

    public long getSum() {
        while (true) {
            final long currentTime = clock.millis();
            final Chunk currentChunk = this.currentChunk.get();
            if (!currentChunk.isExpired(currentTime)) {
                return currentChunk.getSum();
            }

            // current chunk isExpired, let's replace with new
            replaceChunkWithNew();
        }
    }

    public long getSmoothlySum() {
        while (true) {
            final long currentTime = clock.millis();
            final Chunk currentChunk = this.currentChunk.get();
            if (!currentChunk.isExpired(currentTime)) {
                return currentChunk.getSmoothlySum(currentTime, chunkDurationMs);
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
        private final long accumulatedSumExceptThisAndPreviousChunk;
        private final LongAdder sumInPrevChunk;
        private final LongAdder sumInThisChunk = new LongAdder();
        /**
         * byChunkPrevSums[0] - contains value that was in previous to the last chunk in window - needed to smoothing
         * byChunkPrevSums[1] - contains value that was in last chunk in window
         * byChunkPrevSums[2] - contains value that was in the next to the last chunk in window
         * byChunkPrevSums[3] - contains value of chunks in the next to the next to the last chunk in window
         * etc up to previous chunk (exclusive). Previous value can be found in sumInPrevChunk field
         */
        private final long[] sumInPrevChunks;

        private final long expirationTime;

        // if no previous chunk
        private Chunk(int chunkCount, long expirationTime) {
            this.accumulatedSumExceptThisAndPreviousChunk = 0;
            this.sumInPrevChunk = new LongAdder();
            this.sumInPrevChunks = new long[chunkCount - 1 - 1 + 1]; // except this, except previous, include previous to the last
            this.expirationTime = expirationTime;

//            System.out.println("newChunk: " + this.toString());
        }

        private Chunk(long accumulatedSumExceptThisAndPreviousChunk, LongAdder sumInPrevChunk, long[] sumInPrevChunks, long expirationTime) {
            this.accumulatedSumExceptThisAndPreviousChunk = accumulatedSumExceptThisAndPreviousChunk;
            this.sumInPrevChunk = sumInPrevChunk;
            this.sumInPrevChunks = sumInPrevChunks;
            this.expirationTime = expirationTime;

//            System.out.println("newChunk: " + this.toString());
        }

        private long getSum() {
            return accumulatedSumExceptThisAndPreviousChunk + sumInPrevChunk.longValue() + sumInThisChunk.longValue();
        }

        private long getSmoothlySum(long currentTimeMs, long chunkDurationMs) {
            return accumulatedSumExceptThisAndPreviousChunk +
                    sumInPrevChunk.longValue() +
                    sumInThisChunk.longValue() +
                    (long) (getSumInChunkBeforeLastChunkInWindow() * ((expirationTime - currentTimeMs) * 1.0 / chunkDurationMs));
        }

        private void add(long delta) {
            sumInThisChunk.add(delta);
        }

        private boolean isExpired(long currentTime) {
            return expirationTime <= currentTime;
        }

        private Chunk createNext(long chunkDurationMs) {
            final long sumInPrevChunk = getSumInPrevChunk();

            final long newAccumulatedSumExceptThisAndPreviousChunk =
                    accumulatedSumExceptThisAndPreviousChunk
                            - getSumInLastChunkInWindow()
                            + sumInPrevChunk;

            long[] newSumInPrevChunks = new long[sumInPrevChunks.length];
            System.arraycopy(sumInPrevChunks, 1, newSumInPrevChunks, 0, newSumInPrevChunks.length - 1); // shift, add previous to sums
            newSumInPrevChunks[newSumInPrevChunks.length - 1] = sumInPrevChunk;

            final Chunk chunk = new Chunk(
                    newAccumulatedSumExceptThisAndPreviousChunk,
                    sumInThisChunk,
                    newSumInPrevChunks,
                    expirationTime + chunkDurationMs);
            return chunk;
        }

        private long getSumInPrevChunk() {
            return sumInPrevChunk.longValue();
        }

        private long getSumInLastChunkInWindow() {
            return sumInPrevChunks[1];
        }

        private long getSumInChunkBeforeLastChunkInWindow() {
            return sumInPrevChunks[0];
        }

        @Override
        public String toString() {
            return "Chunk{" +
                    "accumulatedSumExceptThisAndPreviousChunk=" + accumulatedSumExceptThisAndPreviousChunk +
                    ", sumInPrevChunk=" + sumInPrevChunk +
                    ", sumInThisChunk=" + sumInThisChunk +
                    ", sumInPrevChunks=" + Arrays.toString(sumInPrevChunks) +
                    ", expirationTime=" + expirationTime +
                    '}';
        }
    }
}
