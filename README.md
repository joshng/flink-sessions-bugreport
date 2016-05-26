# flink-session-bugreport

An example Flink job demonstrating unexpected windowing behavior when timestamps are adjusted mid-stream.

This job performs the following steps:

1. inject a randomized set of input data
1. aggregate the data into `EventTimeSessionWindows`
1. adjust the timestamps for the session-windows via `assignTimestampsAndWatermarks`
1. further aggregate the results into `TumblingEventTimeWindows`

This produces results as expected if step 3 is omitted (demonstrated here by passing '-n' on the command-line). However, the timestamp adjustment leads to multiple disjointed results being emitted for each expected aggregate/time-window.

To see the problem, invoke the job with no command-line arguments:

```sh
mvn exec:java -Dexec.mainClass=com.joshng.flinkSessionsBugReport.FlinkSessionsBugReportJob
```

To see the correct behavior (without step 3), invoke with "-n":

```sh
mvn exec:java -Dexec.mainClass=com.joshng.flinkSessionsBugReport.FlinkSessionsBugReportJob -Dexec.args="-n"
```
