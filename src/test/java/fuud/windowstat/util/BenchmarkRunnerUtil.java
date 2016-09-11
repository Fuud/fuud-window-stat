/*
 *
 *  Copyright 2016 Vladimir Bukhtoyarov
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package fuud.windowstat.util;

import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

public class BenchmarkRunnerUtil {

    public static void runBenchmark(Class benchmarkClass) throws Exception {
        final File tempFile = File.createTempFile("results", "json");

        Options opt = new OptionsBuilder()
                .include(benchmarkClass.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(6))
                .resultFormat(ResultFormatType.JSON)
                .result(tempFile.getAbsolutePath())
                .forks(1)
                .build();
        new Runner(opt).run();

        final String result = new String(Files.readAllBytes(tempFile.toPath()));
//        System.out.println("RESULT");
//        System.out.println(result);

    }

}
