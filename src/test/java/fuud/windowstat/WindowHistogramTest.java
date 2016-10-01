package fuud.windowstat;

import fuud.windowstat.util.MockClock;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public abstract class WindowHistogramTest {

    protected abstract WindowHistogram createHistogram(long[] bucketOffsets, Duration windowSize, int chunkCount, MockClock clock);

    @Test
    public void testHistogram() {
        final MockClock clock = new MockClock();
        long bucketOffsets[] = new long[]{0, 2, 4, 6};
        WindowHistogram windowHistogram = createHistogram(bucketOffsets, Duration.ofSeconds(6), 3, clock);

        /*
                    __________________________________________________________________________________________
                    |Time | Window |                                 Buckets                                  |
                    |     |        | #0 [MinValue, 0) | #1 [0, 2) | #2 [2, 4) |  #3 [4, 6) | #4 [6, MaxValue) |
                    |--------------|--------------------------------------------------------------------------|
         current -> | 0   |        |                  |           |           |            |                  |
                    | 1   |   1    |        0         |     0     |     0     |     0      |        0         |
                    |-----------------------------------------------------------------------------------------|

         */


        assertHistogram(windowHistogram,
                new ReferenceData() {{
                    min = 0;
                    max = 0;
                    mean = 0;
                    samplesCount = 0;

                    overflow = false;
                    underflow = false;

                    percentile_00 = 0;
                    percentile_10 = 0;
                    percentile_20 = 0;
                    percentile_30 = 0;
                    percentile_40 = 0;
                    percentile_50 = 0;
                    percentile_60 = 0;
                    percentile_70 = 0;
                    percentile_80 = 0;
                    percentile_100 = 0;
                }});

        //-----------------------------------

        clock.move(1000);
        /*
                    __________________________________________________________________________________________
                    |Time | Window |                                 Buckets                                  |
                    |     |        | #0 [MinValue, 0) | #1 [0, 2) | #2 [2, 4) |  #3 [4, 6) | #4 [6, MaxValue) |
                    |--------------|--------------------------------------------------------------------------|
                    | 0   |        |                  |           |           |            |                  |
         current -> | 1   |   1    |        0         |     0     |     0     |     0      |        0         |
                    |-----------------------------------------------------------------------------------------|

         */
        assertHistogram(windowHistogram,
                new ReferenceData() {{
                    min = 0;
                    max = 0;
                    mean = 0;
                    samplesCount = 0;

                    overflow = false;
                    underflow = false;

                    percentile_00 = 0;
                    percentile_10 = 0;
                    percentile_20 = 0;
                    percentile_30 = 0;
                    percentile_40 = 0;
                    percentile_50 = 0;
                    percentile_60 = 0;
                    percentile_70 = 0;
                    percentile_80 = 0;
                    percentile_100 = 0;
                }});

        //-----------------------------------
        windowHistogram.add(5);
        /*
                    __________________________________________________________________________________________
                    |Time | Window |                                 Buckets                                  |
                    |     |        | #0 [MinValue, 0) | #1 [0, 2) | #2 [2, 4) |  #3 [4, 6) | #4 [6, MaxValue) |
                    |--------------|--------------------------------------------------------------------------|
                    | 0   |        |                  |           |           |            |                  |
         current -> | 1   |   1    |        0         |     0     |     0     |     1      |        0         |
                    |-----------------------------------------------------------------------------------------|

         */
        assertHistogram(windowHistogram,
                new ReferenceData() {{
                    min = 5;
                    max = 5;
                    mean = 5;
                    samplesCount = 1;

                    overflow = false;
                    underflow = false;

                    // only one sample - it will meet all percentiles.
                    // Real value is 5, but histogram column from 4 to 6. Let's take bottom value.
                    percentile_00 = 4;
                    percentile_10 = 4;
                    percentile_20 = 4;
                    percentile_30 = 4;
                    percentile_40 = 4;
                    percentile_50 = 4;
                    percentile_60 = 4;
                    percentile_70 = 4;
                    percentile_80 = 4;
                    percentile_90 = 4;
                    percentile_100 = 4;
                }});


        //-----------------------------------
        windowHistogram.add(1);
        /*
                    __________________________________________________________________________________________
                    |Time | Window |                                 Buckets                                  |
                    |     |        | #0 [MinValue, 0) | #1 [0, 2) | #2 [2, 4) |  #3 [4, 6) | #4 [6, MaxValue) |
                    |--------------|--------------------------------------------------------------------------|
                    | 0   |        |                  |           |           |            |                  |
         current -> | 1   |   1    |        0         |     1     |     0     |     1      |        0         |
                    |-----------------------------------------------------------------------------------------|

         */
        assertHistogram(windowHistogram,
                new ReferenceData() {{
                    min = 1;
                    max = 5;
                    mean = 3;
                    samplesCount = 2;

                    overflow = false;
                    underflow = false;

                    percentile_00 = 0;
                    percentile_10 = 0;
                    percentile_20 = 0;
                    percentile_30 = 0;
                    percentile_40 = 0;
                    percentile_50 = 0;
                    percentile_60 = 0;
                    percentile_70 = 0;
                    percentile_80 = 0;
                    percentile_90 = 0;
                    percentile_100 = 4;
                }});


        //-----------------------------------
        windowHistogram.add(1);
        windowHistogram.add(1);
        windowHistogram.add(1);
        windowHistogram.add(1);
        windowHistogram.add(5);
        windowHistogram.add(5);
        windowHistogram.add(5);
        windowHistogram.add(5);
        /*
                    __________________________________________________________________________________________
                    |Time | Window |                                 Buckets                                  |
                    |     |        | #0 [MinValue, 0) | #1 [0, 2) | #2 [2, 4) |  #3 [4, 6) | #4 [6, MaxValue) |
                    |--------------|--------------------------------------------------------------------------|
                    | 0   |        |                  |           |           |            |                  |
         current -> | 1   |   1    |        0         |     5     |     0     |     5      |        0         |
                    |-----------------------------------------------------------------------------------------|

         */
        assertHistogram(windowHistogram,
                new ReferenceData() {{
                    min = 1;
                    max = 5;
                    mean = 3;
                    samplesCount = 10;

                    overflow = false;
                    underflow = false;

                    percentile_00 = 0;
                    percentile_10 = 0;
                    percentile_20 = 0;
                    percentile_30 = 0;
                    percentile_40 = 0;
                    percentile_50 = 0;
                    percentile_60 = 4;
                    percentile_70 = 4;
                    percentile_80 = 4;
                    percentile_90 = 4;
                    percentile_100 = 4;
                }});


        //-----------------------------------
        windowHistogram.add(-8);
        /*
                    __________________________________________________________________________________________
                    |Time | Window |                                 Buckets                                  |
                    |     |        | #0 [MinValue, 0) | #1 [0, 2) | #2 [2, 4) |  #3 [4, 6) | #4 [6, MaxValue) |
                    |--------------|--------------------------------------------------------------------------|
                    | 0   |        |                  |           |           |            |                  |
         current -> | 1   |   1    |        1         |     5     |     0     |     5      |        0         |
                    |-----------------------------------------------------------------------------------------|

         */
        assertHistogram(windowHistogram,
                new ReferenceData() {{
                    min = -8;
                    max = 5;
                    mean = 2; //(5*5+1*5+(-8))/11
                    samplesCount = 11;

                    overflow = false;
                    underflow = true;

                    percentile_00 = Long.MIN_VALUE; //underflow
                    percentile_10 = Long.MIN_VALUE;
                    percentile_20 = 0;
                    percentile_30 = 0;
                    percentile_40 = 0;
                    percentile_50 = 0;
                    percentile_60 = 0;
                    percentile_70 = 4;
                    percentile_80 = 4;
                    percentile_90 = 4;
                    percentile_100 = 4;
                }});


        //-----------------------------------
        windowHistogram.add(14);
        /*
                    __________________________________________________________________________________________
                    |Time | Window |                                 Buckets                                  |
                    |     |        | #0 [MinValue, 0) | #1 [0, 2) | #2 [2, 4) |  #3 [4, 6) | #4 [6, MaxValue) |
                    |--------------|--------------------------------------------------------------------------|
                    | 0   |        |                  |           |           |            |                  |
         current -> | 1   |   1    |        1         |     5     |     0     |     5      |        1         |
                    |-----------------------------------------------------------------------------------------|

         */
        assertHistogram(windowHistogram,
                new ReferenceData() {{
                    min = -8;
                    max = 14;
                    mean = 3; //(5*5 + 1*5 + (-8) + 14)/12
                    samplesCount = 12;

                    overflow = true;
                    underflow = true;

                    percentile_00 = Long.MIN_VALUE; //underflow
                    percentile_10 = Long.MIN_VALUE;
                    percentile_20 = 0;
                    percentile_30 = 0;
                    percentile_40 = 0;
                    percentile_50 = 0;
                    percentile_60 = 4;
                    percentile_70 = 4;
                    percentile_80 = 4;
                    percentile_90 = 4; //overflow
                    percentile_100 = Long.MAX_VALUE; //overflow
                }});


        //-----------------------------------
        windowHistogram.add(2); // should increment bucket #2
        /*
                    __________________________________________________________________________________________
                    |Time | Window |                                 Buckets                                  |
                    |     |        | #0 [MinValue, 0) | #1 [0, 2) | #2 [2, 4) |  #3 [4, 6) | #4 [6, MaxValue) |
                    |--------------|--------------------------------------------------------------------------|
                    | 0   |        |                  |           |           |            |                  |
         current -> | 1   |   1    |        1         |     5     |     1     |     5      |        0         |
                    |-----------------------------------------------------------------------------------------|

         */
        assertHistogram(windowHistogram,
                new ReferenceData() {{
                    min = -8;
                    max = 14;
                    mean = 2; //(5*5 + 1*5 + (-8) + 14 + 2)/13
                    samplesCount = 13;

                    overflow = true;
                    underflow = true;

                    percentile_00 = Long.MIN_VALUE; //underflow
                    percentile_10 = Long.MIN_VALUE;
                    percentile_20 = 0;
                    percentile_30 = 0;
                    percentile_40 = 0;
                    percentile_50 = 0;
                    percentile_60 = 2;
                    percentile_70 = 4;
                    percentile_80 = 4;
                    percentile_90 = 4; //overflow
                    percentile_100 = Long.MAX_VALUE; //overflow
                }});
    }

    private void assertHistogram(WindowHistogram windowHistogram, ReferenceData referenceData) {
        assertEquals(referenceData.max, windowHistogram.getMax());
        assertEquals(referenceData.min, windowHistogram.getMin());
        assertEquals(referenceData.mean, windowHistogram.getMean());
        assertEquals(referenceData.samplesCount, windowHistogram.getSamplesCount());
        assertEquals(referenceData.percentile_00, windowHistogram.getPercentile(0.0));
        assertEquals(referenceData.percentile_10, windowHistogram.getPercentile(0.1));
        assertEquals(referenceData.percentile_20, windowHistogram.getPercentile(0.2));
        assertEquals(referenceData.percentile_30, windowHistogram.getPercentile(0.3));
        assertEquals(referenceData.percentile_40, windowHistogram.getPercentile(0.4));
        assertEquals(referenceData.percentile_50, windowHistogram.getPercentile(0.5));
        assertEquals(referenceData.percentile_60, windowHistogram.getPercentile(0.6));
        assertEquals(referenceData.percentile_70, windowHistogram.getPercentile(0.7));
        assertEquals(referenceData.percentile_80, windowHistogram.getPercentile(0.8));
        assertEquals(referenceData.percentile_90, windowHistogram.getPercentile(0.9));
        assertEquals(referenceData.percentile_100, windowHistogram.getPercentile(1.0));

        assertEquals(referenceData.overflow, windowHistogram.isOverflow());
        assertEquals(referenceData.underflow, windowHistogram.isUnderflow());


        for (double i = 0.0; i < 0.10; i = i + 0.1) {
            assertTrue(windowHistogram.getPercentile(i) >= referenceData.percentile_00);
            assertTrue(windowHistogram.getPercentile(i) <= referenceData.percentile_10);
        }
        for (double i = 0.1; i < 0.20; i = i + 0.1) {
            assertTrue(windowHistogram.getPercentile(i) >= referenceData.percentile_10);
            assertTrue(windowHistogram.getPercentile(i) <= referenceData.percentile_20);
        }
        for (double i = 0.2; i < 0.30; i = i + 0.1) {
            assertTrue(windowHistogram.getPercentile(i) >= referenceData.percentile_20);
            assertTrue(windowHistogram.getPercentile(i) <= referenceData.percentile_30);
        }
        for (double i = 0.3; i < 0.40; i = i + 0.1) {
            assertTrue(windowHistogram.getPercentile(i) >= referenceData.percentile_30);
            assertTrue(windowHistogram.getPercentile(i) <= referenceData.percentile_40);
        }
        for (double i = 0.4; i < 0.50; i = i + 0.1) {
            assertTrue(windowHistogram.getPercentile(i) >= referenceData.percentile_40);
            assertTrue(windowHistogram.getPercentile(i) <= referenceData.percentile_50);
        }
        for (double i = 0.5; i < 0.60; i = i + 0.1) {
            assertTrue(windowHistogram.getPercentile(i) >= referenceData.percentile_50);
            assertTrue(windowHistogram.getPercentile(i) <= referenceData.percentile_60);
        }
        for (double i = 0.6; i < 0.70; i = i + 0.1) {
            assertTrue(windowHistogram.getPercentile(i) >= referenceData.percentile_60);
            assertTrue(windowHistogram.getPercentile(i) <= referenceData.percentile_70);
        }
        for (double i = 0.7; i < 0.80; i = i + 0.1) {
            assertTrue(windowHistogram.getPercentile(i) >= referenceData.percentile_70);
            assertTrue(windowHistogram.getPercentile(i) <= referenceData.percentile_80);
        }
        for (double i = 0.8; i < 0.90; i = i + 0.1) {
            assertTrue(windowHistogram.getPercentile(i) >= referenceData.percentile_80);
            assertTrue(windowHistogram.getPercentile(i) <= referenceData.percentile_90);
        }
        for (double i = 0.9; i < 1.0; i = i + 0.1) {
            assertTrue(windowHistogram.getPercentile(i) >= referenceData.percentile_90);
            assertTrue(windowHistogram.getPercentile(i) <= referenceData.percentile_100);
        }
    }

    private class ReferenceData {
        long min;
        long max;
        long mean;
        long samplesCount;
        long percentile_00;
        long percentile_10;
        long percentile_20;
        long percentile_30;
        long percentile_40;
        long percentile_50;
        long percentile_60;
        long percentile_70;
        long percentile_80;
        long percentile_90;
        long percentile_100;
        boolean overflow;
        boolean underflow;
    }

}