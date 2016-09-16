package fuud.windowstat;

import fuud.windowstat.util.MockClock;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class WindowMinMaxTest {
    @Test
    public void test() throws Exception {
        MockClock clock = new MockClock();

        WindowMinMax counter = new WindowMinMax(Duration.ofSeconds(3), 3, clock);
        /*
         * Chunk:     |    1    |
         * Time:      |0        |1000
         * Val:       |0        |
         *             A
         *             |
         *             |
         *       we are here
         *
         */
        assertEquals(0, counter.getMin());
        assertEquals(0, counter.getMax());

        //--------------------------------

        counter.register(100);
        /*
         * Chunk:     |    1    |
         * Time:      |0        |1000
         * Val:       |100      |
         *             A
         *             |
         *             |
         *       we are here
         *
         */
        assertEquals(100, counter.getMin());
        assertEquals(100, counter.getMax());

        //--------------------------------

        clock.setTime(2600);
        /*
         *            |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |
         * Time:      |0        |1000     |2000     |
         * Val:       |100      |0        |0        |
         *                                      A
         *                                      |
         *                                      |
         *                                we are here, all chunks in under window
         *
         */
        assertEquals(100, counter.getMin());
        assertEquals(100, counter.getMax());

        //--------------------------------

        clock.setTime(3600);
        /*
         *                      |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |
         * Time:      |0        |1000     |2000     |3000     |
         * Val:       |100      |0        |0        |0        |
         *                                               A
         *                                               |
         *                                               |
         *                                         we are here
         *
         */
        assertEquals(0, counter.getMin());
        assertEquals(0, counter.getMax());

        //--------------------------------
        //--------------------------------

        clock.setTime(4000);
        /*
         *                                |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |    5    |
         * Time:      |0        |1000     |2000     |3000     |4000     |
         * Val:       |100      |0        |0        |0        |0        |
         *                                                     A
         *                                                     |
         *                                                     |
         *                                               we are here, all sums is zero
         *
         */
        assertEquals(0, counter.getMin());
        assertEquals(0, counter.getMax());

        //--------------------------------

        counter.register(200);
        /*
         *                                |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |    5    |
         * Time:      |0        |1000     |2000     |3000     |4000     |
         * Val:       |100      |0        |0        |0        |200      |
         *                                                     A
         *                                                     |
         *                                                     |
         *                                               we are here
         *
         */
        assertEquals(200, counter.getMin());
        assertEquals(200, counter.getMax());

        //--------------------------------

        clock.setTime(5000);
        /*
         *                                          |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |    5    |    6    |
         * Time:      |0        |1000     |2000     |3000     |4000     |5000     |
         * Val:       |100      |0        |0        |0        |200      |0        |
         *                                                               A
         *                                                               |
         *                                                               |
         *                                                         we are here
         *
         */
        assertEquals(200, counter.getMin());
        assertEquals(200, counter.getMax());

        //--------------------------------

        counter.register(300);
        /*
         *                                          |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |    5    |    6    |
         * Time:      |0        |1000     |2000     |3000     |4000     |5000     |
         * Val:       |100      |0        |0        |0        |200      |300      |
         *                                                               A
         *                                                               |
         *                                                               |
         *                                                         we are here
         *
         */
        assertEquals(200, counter.getMin());
        assertEquals(300, counter.getMax());

        //--------------------------------

        clock.setTime(6000);
        counter.register(200);
        /*
         *                                                    |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |    5    |    6    |    7    |
         * Time:      |0        |1000     |2000     |3000     |4000     |5000     |6000     |
         * Val:       |100      |0        |0        |0        |200      |300      |200      |
         *                                                                        A
         *                                                                        |
         *                                                                        |
         *                                                                  we are here
         *
         */
        assertEquals(200, counter.getMin());
        assertEquals(300, counter.getMax());

        //--------------------------------

        clock.setTime(8000);
        /*
         *                                                                        |<---------- window --------->|
         *
         * Chunk:     |    1    |    2    |    3    |    4    |    5    |    6    |    7    |    8    |    9    |
         * Time:      |0        |1000     |2000     |3000     |4000     |5000     |6000     |7000     |8000     |
         * Val:       |100      |0        |0        |0        |200      |300      |200      |200      |200      |
         *                                                                                            A
         *                                                                                            |
         *                                                                                            |
         *                                                                                      we are here
         *
         */
        assertEquals(200, counter.getMin());
        assertEquals(200, counter.getMax());

        //--------------------------------
    }
}