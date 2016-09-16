/*
 *
 *  Copyright THREAD_PER_OPERATION016 Vladimir Bukhtoyarov
 *
 *    Licensed under the Apache License, Version THREAD_PER_OPERATION.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-THREAD_PER_OPERATION.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package fuud.windowstat;

import fuud.windowstat.util.BenchmarkRunnerUtil;
import org.openjdk.jmh.annotations.*;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class WindowMaxMinBenchmark {
    private static final int THREAD_PER_OPERATION = 4;

    @State(Scope.Group)
    public static class WindowMinMaxState {
        public final WindowMinMax counter = new WindowMinMax(Duration.ofSeconds(3), 3, Clock.systemDefaultZone());
    }

    @State(Scope.Thread)
    public static class WindowMinMaxData {
        public final long[] data = new long[10000];
        public int elemIndex;

        @Setup
        public void setup(){
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i=0; i<data.length; i++){
                data[i] = random.nextLong(data.length);
            }
        }

        public long nextValue(){
            elemIndex++;
            return data[elemIndex % data.length];
        }
    }

    @State(Scope.Benchmark)
    public static class IncrementAtomicState {
        AtomicLong sum = new AtomicLong();
    }

    @State(Scope.Benchmark)
    public static class IncrementLongAdder {
        LongAdder sum = new LongAdder();
    }

    // tests

    @Benchmark
    @Threads(THREAD_PER_OPERATION)
    public long baseLineCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Benchmark
    @Threads(THREAD_PER_OPERATION)
    public long baseLineIncrementAtomic(IncrementAtomicState state) {
        return state.sum.addAndGet(1);
    }

    @Benchmark
    @Threads(THREAD_PER_OPERATION)
    public void baseLineIncrementLongAdder(IncrementLongAdder state) {
        state.sum.add(1L);
    }

    @Benchmark
    @Threads(THREAD_PER_OPERATION)
    public long baseLineIncrementAndGetLongAdder(IncrementLongAdder state) {
        state.sum.add(1L);
        return state.sum.longValue();
    }

    @Benchmark
    @Group("window_counter_add_read")
    @GroupThreads(THREAD_PER_OPERATION)
    public void benchmarkAddRead_read(WindowMinMaxState state) {
        state.counter.getMin();
    }

    @Benchmark
    @Group("window_counter_add_read")
    @GroupThreads(THREAD_PER_OPERATION)
    public void benchmarkAddRead_write(WindowMinMaxState state, WindowMinMaxData datas) {
        state.counter.register(datas.nextValue());
    }


    public static class RunBenchmark {
        public static void main(String[] args) throws Exception {
            BenchmarkRunnerUtil.runBenchmark(WindowMaxMinBenchmark.class);
        }
    }

}
