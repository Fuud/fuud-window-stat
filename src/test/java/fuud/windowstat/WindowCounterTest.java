package fuud.windowstat;

import fuud.windowstat.util.MockClock;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class WindowCounterTest {
    @Test
    public void testAddAndCalculateSum() throws Exception {
        MockClock clock = new MockClock();

        WindowCounter counter = new WindowCounter(Duration.ofSeconds(3), 3, clock);
        /*
         * Chunk:     |    1    |
         * Time:      |0        |1000
         * Sum:       |0        |
         *             /\
         *             |
         *             |
         *       we are here
         *
         */
        assertEquals(0, counter.getSum());
        assertEquals(0, counter.getSmoothlySum());

        //--------------------------------

        counter.add(100);
        /*
         * Chunk:     |    1    |
         * Time:      |0        |1000
         * Sum:       |100      |
         *             /\
         *             |
         *             |
         *       we are here
         *
         */
        assertEquals(100, counter.getSum());
        assertEquals(100, counter.getSmoothlySum());

        //--------------------------------

        clock.setTime(2600);
        /*
         *            |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |
         * Time:      |0        |1000     |2000     |
         * Sum:       |100      |0        |0        |
         *                                      /\
         *                                      |
         *                                      |
         *                                we are here, all chunks in under window, sum = 100
         *
         */
        assertEquals(100, counter.getSum());
        assertEquals(100, counter.getSmoothlySum());

        //--------------------------------

        clock.setTime(3600);
        /*
         *                      |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |
         * Time:      |0        |1000     |2000     |3000     |
         * Sum:       |100      |0        |0        |0        |
         *                                               /\
         *                                               |
         *                                               |
         *                                         we are here,
         *                                         by-chunk-sum is equal to zero because no measurements was recorded within time window
         *                                         smooth sum: current chunk is (600/1000)=60% done. Let's add 40% from chunk #1 that is previous to first chunk in time window
         *
         */
        assertEquals(0, counter.getSum());
        assertEquals(40, counter.getSmoothlySum());

        //--------------------------------

        clock.setTime(3980);
        /*
         *                      |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |
         * Time:      |0        |1000     |2000     |3000     |
         * Sum:       |100      |0        |0        |0        |
         *                                                  /\
         *                                                  |
         *                                                  |
         *                                            we are here,
         *                                            by-chunk-sum is equal to zero because no measurements was recorded within time window
         *                                            smooth sum: current chunk is (980/1000)=98% done. Let's add 2% from chunk #1 (that is previous to first chunk in time window)
         *
         */
        assertEquals(0, counter.getSum());
        assertEquals(2, counter.getSmoothlySum());

        //--------------------------------

        clock.setTime(4000);
        /*
         *                                |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |    5    |
         * Time:      |0        |1000     |2000     |3000     |4000     |
         * Sum:       |100      |0        |0        |0        |0        |
         *                                                     /\
         *                                                     |
         *                                                     |
         *                                               we are here, all sums is zero
         *
         */
        assertEquals(0, counter.getSum());
        assertEquals(0, counter.getSmoothlySum());

        //--------------------------------

        counter.add(200);
        /*
         *                                |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |    5    |
         * Time:      |0        |1000     |2000     |3000     |4000     |
         * Sum:       |100      |0        |0        |0        |200      |
         *                                                     /\
         *                                                     |
         *                                                     |
         *                                               we are here, all sums is equal to 200
         *
         */
        assertEquals(200, counter.getSum());
        assertEquals(200, counter.getSmoothlySum());

        //--------------------------------

        clock.setTime(5000);
        /*
         *                                          |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |    5    |    6    |
         * Time:      |0        |1000     |2000     |3000     |4000     |5000     |
         * Sum:       |100      |0        |0        |0        |200      |0        |
         *                                                               /\
         *                                                               |
         *                                                               |
         *                                                         we are here, all sums is equal to 200
         *
         */
        assertEquals(200, counter.getSum());
        assertEquals(200, counter.getSmoothlySum());

        //--------------------------------

        counter.add(300);
        /*
         *                                          |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |    5    |    6    |
         * Time:      |0        |1000     |2000     |3000     |4000     |5000     |
         * Sum:       |100      |0        |0        |0        |200      |300      |
         *                                                               /\
         *                                                               |
         *                                                               |
         *                                                         we are here, all sums is equal to 500
         *
         */
        assertEquals(500, counter.getSum());
        assertEquals(500, counter.getSmoothlySum());

        //--------------------------------
    }
}