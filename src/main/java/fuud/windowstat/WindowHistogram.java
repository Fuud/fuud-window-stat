package fuud.windowstat;

import java.util.stream.LongStream;

public interface WindowHistogram {
    long getMax();

    long getMin();

    long getMean();

    long getPercentile(double percentile);

    boolean isOverflow();

    boolean isUnderflow();

    CounterWindowHistogram.Bucket[] getSnapshot();

    long getSamplesCount();

    void add(long value);

    class Snapshot{
        private final long min;
        private final long max;
        private final long mean;
        private final long samplesCount;
        private final long[] bucketOffsets;
        private final long[] bucketValues;

        public Snapshot(long min, long max, long[] bucketOffsets, long[] bucketValues, long totalSum) {
            this.min = min;
            this.max = max;
            this.samplesCount = LongStream.of(bucketValues).sum();
            this.mean = (long)(totalSum*1.0/samplesCount);
            this.bucketOffsets = bucketOffsets;
            this.bucketValues = bucketValues;
        }

        long getMax(){
            return max;
        }

        long getMin(){
            return min;
        }

        long getMean(){
            return mean;
        }

        long getSamplesCount(){
            return samplesCount;
        }

        long getPercentile(double percentile){
            long count = 0;
            for (long bucket : bucketValues) {
                count += bucket;
            }

            if (count == 0) {
                return 0;
            } else {
                int countUnderPercentile = (int) (count * percentile); // math round
                if (countUnderPercentile == 0) {
                    countUnderPercentile = 1;
                }
                count = 0;
                for (int i = 0; i < bucketValues.length; i++) {
                    count += bucketValues[i];
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

        boolean isOverflow(){
            return bucketValues[bucketValues.length - 1] > 0;
        }

        boolean isUnderflow(){
            return bucketValues[0] > 0;
        }
    }
}
