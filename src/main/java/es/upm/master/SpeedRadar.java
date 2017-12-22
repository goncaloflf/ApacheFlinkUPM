package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.awt.event.WindowEvent;
import java.util.*;

//INPUT FORMAT: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos

public class SpeedRadar {

    public static void main(String[] args)  throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(10);

        String	inFilePath = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\traffic-3xways";
        String	outFilePathRadar = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\speedfines.csv";
        String	outFilePathAccident = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\accidents.csv";
        String	outFilePathAverage = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\avgspeedfines.csv";
        DataStream<String> source = env.readTextFile(inFilePath).setParallelism(1);

        Map<Integer, ArrayList<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>> samePos = new HashMap<>();

        //Splits the lines by commas, discards the lines with speed under 91. Parses the String to a tuple of integers.
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> speedRadar = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                String[] s = value.split(",");
                int speed = Integer.parseInt(s[2]);
                return speed > 90;
            }
        }).map(new MapFunction<String, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
            @Override
            public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> map(String value) {
                String[] s = value.split(",");
                return new Tuple8<>(Integer.parseInt(s[0]),Integer.parseInt(s[1]),Integer.parseInt(s[2]),
                        Integer.parseInt(s[3]),Integer.parseInt(s[4]),Integer.parseInt(s[5]),Integer.parseInt(s[6]),
                        Integer.parseInt(s[7]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                return element.f0 * 1000;
            }
        });

        final DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> DataStreamtuples = source.
                map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String value) {
                        String[] s = value.split(",");
                        return new Tuple8<>(Integer.parseInt(s[0]), Integer.parseInt(s[1]), Integer.parseInt(s[2]),
                                Integer.parseInt(s[3]), Integer.parseInt(s[4]), Integer.parseInt(s[5]), Integer.parseInt(s[6]),
                                Integer.parseInt(s[7]));
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                        return element.f0;
                    }
                });


        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> AvgSpeedControl = DataStreamtuples

                .filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple_filter) throws Exception {
                        int seg = tuple_filter.f6;
                        return seg >= 52 && seg <= 56;
                    }
                })

                .keyBy(1,3,5)
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .apply(new averageSpeedFunction());

        KeyedStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedAccident =  source
                .map(new MapFunction<String, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
                    @Override
                    public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> map(String value) {
                        String[] s = value.split(",");
                        return new Tuple8<>(Integer.parseInt(s[0]),Integer.parseInt(s[1]),Integer.parseInt(s[2]),
                                Integer.parseInt(s[3]),Integer.parseInt(s[4]),Integer.parseInt(s[5]),Integer.parseInt(s[6]),
                                Integer.parseInt(s[7]));
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                        return element.f0 * 1000;
                    }
                })
                .keyBy(1,7);

        SingleOutputStreamOperator<Object> accidentReporter = keyedAccident.window(SlidingEventTimeWindows.of(Time.seconds(120), Time.seconds(30))).apply(new AccidentChecker()).setParallelism(1);


        speedRadar.writeAsCsv(outFilePathRadar, FileSystem.WriteMode.OVERWRITE);
        AvgSpeedControl.writeAsCsv(outFilePathAverage, FileSystem.WriteMode.OVERWRITE);
        accidentReporter.writeAsText(outFilePathAccident, FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }


    private static class averageSpeedFunction implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> collector) throws Exception {
            boolean segA = false, segB = false;
            Tuple stream;
            List<Integer> timestamp = new ArrayList<Integer>();
            List<Integer> Distance = new ArrayList<Integer>();
            List<Integer> speed = new ArrayList<Integer>();
            int Time2 = 0, Time1 = 0;
            int Distance1 = 0, Distance2 = 0;

            OptionalDouble avgSpeed = OptionalDouble.of(0);
            Double avgSpeedms = 0.0;
            Double avgSpeedFinal = 0.0;
            for (Tuple8 element : iterable) {
                if ((int) element.f6 == 52) {
                    segA = true;
                    timestamp.add((int) element.f0);
                    Distance.add((int) element.f7);

                } else if ((int) element.f6 == 56) {
                    segB = true;
                    timestamp.add((int) element.f0);
                    Distance.add((int) element.f7);
                }
                speed.add((int) element.f2);

                if (segA && segB) {
                    Time2 = Collections.max(timestamp);
                    Time1 = Collections.min(timestamp);

                    Distance2 = Collections.max(Distance);
                    Distance1 = Collections.min(Distance);

                    avgSpeedms = (Distance2 - Distance1) / (Time2 - Time1) + 0.0;
                    avgSpeedFinal = avgSpeedms * 2.237;

                }
            }
            if (avgSpeedFinal > 60.0)
                collector.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>(Time1, Time2, Integer.parseInt(tuple.getField(0).toString()), Integer.parseInt(tuple.getField(1).toString()), Integer.parseInt(tuple.getField(2).toString()), avgSpeedFinal));
        }
    }

    private static class AccidentChecker implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Object, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable, Collector<Object> collector) throws Exception {
            int count = 0, lastPos = 0;
            for(Tuple8 element: iterable) {
                if(count == 0) {
                    lastPos = (int) element.f7;
                    count = 1;
                    continue;
                }
                int currPos = (int) element.f7;
                if(currPos == lastPos) {
                    count += 1;
                    if(count >= 4) {
                        collector.collect(new Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>(
                                (int) element.f0 - 120, (int) element.f0, (int) element.f1, (int) element.f3, (int) element.f5, (int) element.f6, (int) element.f7));
                    }
                } else {
                    count = 0;
                    lastPos = 0;
                }
            }
        }
    }
}
