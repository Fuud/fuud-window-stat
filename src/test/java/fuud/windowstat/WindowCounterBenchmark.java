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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class WindowCounterBenchmark {
    private static final int THREAD_PER_OPERATION = 4;

    @State(Scope.Group)
    public static class CounterState {

        public final WindowCounter counter = new WindowCounter(Duration.ofSeconds(3), 3, Clock.systemDefaultZone());
    }

    @State(Scope.Benchmark)
    @Threads(THREAD_PER_OPERATION)
    public static class IncrementAtomicState {
        AtomicLong sum = new AtomicLong();
    }

    @State(Scope.Benchmark)
    @Threads(THREAD_PER_OPERATION)
    public static class IncrementLongAdder {
        LongAdder sum = new LongAdder();
    }

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
    @Group("window_counter_add")
    @GroupThreads(THREAD_PER_OPERATION)
    public void benchmarkAddToCounter(CounterState state) {
        state.counter.add(1);
    }

    @Benchmark
    @Group("window_counter_add_read")
    @GroupThreads(THREAD_PER_OPERATION)
    public void benchmarkAddRead_add(CounterState state) {
        state.counter.add(1);
    }

    @Benchmark
    @Group("window_counter_add_read")
    @GroupThreads(THREAD_PER_OPERATION)
    public void benchmarkAddRead_read(CounterState state) {
        state.counter.getSum();
    }
    
    @Benchmark
    @Group("window_counter_add_read_smooth")
    @GroupThreads(THREAD_PER_OPERATION)
    public void benchmarkAddRead_add_smooth(CounterState state) {
        state.counter.add(1);
    }

    @Benchmark
    @Group("window_counter_add_read_smooth")
    @GroupThreads(THREAD_PER_OPERATION)
    public void benchmarkAddRead_read_smooth(CounterState state) {
        state.counter.getSum();
    }

    public static class RunBenchmark {
        public static void main(String[] args) throws Exception {
            BenchmarkRunnerUtil.runBenchmark(WindowCounterBenchmark.class);
        }
    }

}
