package master2017.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;

//INPUT FORMAT: Time, VID, Spd, XWay, Lane, Dir, Seg, Pos

public class VehicleTelematics {

    public static void main(String[] args)  throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(10);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String inFilePath = "";
        String outFilePath = "";
        try {
            inFilePath = args[0];
            outFilePath = args[1];
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Input file and output folder path must be provided as argument to this program. Aborting...");
            return;
        }
        String	outFilePathRadar = outFilePath +  "/speedfines.csv";
        String	outFilePathAccident = outFilePath + "/accidents.csv";
        String	outFilePathAverage = outFilePath + "/avgspeedfines.csv";
        DataStream<String> source = env.readTextFile(inFilePath).setParallelism(1);

        //Splits the lines by commas, discards the lines with speed under 91. Parses the String to a tuple of integers.
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedRadar = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                String[] s = value.split(",");
                int speed = Integer.parseInt(s[2]);
                return speed > 90;
            }
        }).map(new MapFunction<String, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>() {
            @Override
            public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map(String value) {
                String[] s = value.split(",");
                return new Tuple6<>(Integer.parseInt(s[0]),Integer.parseInt(s[1]),Integer.parseInt(s[3]),
                        Integer.parseInt(s[6]),Integer.parseInt(s[5]),Integer.parseInt(s[2]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> element) {
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
                });

        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> AvgSpeedControl = DataStreamtuples
                .filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple_filter) throws Exception {
                        int seg = tuple_filter.f6;
                        return seg >= 52 && seg <= 56;
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                        return element.f0 * 1000;
                    }
                })
                .keyBy(1,3,5)
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .apply(new averageSpeedFunction());

        SingleOutputStreamOperator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> accidentReporter =  source
                .map(new MapFunction<String, Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {
                    @Override
                    public Tuple8<Integer,Integer,Integer,Integer,Integer,Integer,Integer,Integer> map(String value) {
                        String[] s = value.split(",");
                        return new Tuple8<>(Integer.parseInt(s[0]),Integer.parseInt(s[1]),Integer.parseInt(s[2]),
                                Integer.parseInt(s[3]),Integer.parseInt(s[4]),Integer.parseInt(s[5]),Integer.parseInt(s[6]),
                                Integer.parseInt(s[7]));
                    }
                }).setParallelism(1)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                        return element.f0 * 1000;
                    }
                }).setParallelism(1)
                .keyBy(1,7).countWindow(4,1).apply(new AccidentChecker()).setParallelism(1);

        speedRadar.writeAsCsv(outFilePathRadar, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        AvgSpeedControl.writeAsCsv(outFilePathAverage, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        accidentReporter.writeAsCsv(outFilePathAccident, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
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
            int Time2 = 0, Time1 = Integer.MAX_VALUE;
            int Distance1= Integer.MAX_VALUE, Distance2= 0;

            OptionalDouble avgSpeed = OptionalDouble.of(0);
            Double avgSpeedms =0.0;
            Double avgSpeedFinal =0.0;
            for (Tuple8 element : iterable) {
                if ((int) element.f6 == 52) {
                    segA = true;
                } else if ((int) element.f6 == 56) {
                    segB = true;
                }

                speed.add((int) element.f2);
                timestamp.add((int) element.f0);
                Distance.add((int) element.f7);

                if (segA && segB) {
                    Time2 = Collections.max(timestamp);
                    Time1 = Collections.min(timestamp);

                    Distance2 = Collections.max(Distance);
                    Distance1 = Collections.min(Distance);

                    //Double variable = Calculate the speed which velocity = distance2 - distance 1 / time2 - time1
                    //Distnace*2.237

                    avgSpeedms = (Distance2-Distance1)/(Time2-Time1)+0.0;
                    avgSpeedFinal = avgSpeedms*2.23694;
                    //counter +1
                }
                //Divide sum of velocity/count
            }
            if(avgSpeedFinal > 60.0)
                collector.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>(Time1, Time2, Integer.parseInt(tuple.getField(0).toString()), Integer.parseInt(tuple.getField(1).toString()), Integer.parseInt(tuple.getField(2).toString()), avgSpeedFinal));
        }
    }

    private static class AccidentChecker implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple, GlobalWindow> {
        @Override
        public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable, Collector<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> collector) throws Exception {
            Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> aux = iterable.iterator();
            int time1 = 0, time2 = 0, vid = 0, xway = 0, seg = 0, dir = 0, pos = 0;
            int cont = 0;
            for(Tuple8 item : iterable) {
                if(cont == 0) {
                    time1 = (int) item.f0;
                } else if(cont == 3) {
                    time2 = (int) item.f0;
                    vid = (int) item.f1;
                    xway = (int) item.f3;
                    seg = (int) item.f6;
                    dir = (int) item.f5;
                    pos = (int) item.f7;
                }
                cont++;
            }
            if(cont == 4) {
                collector.collect(new Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>(time1, time2, vid, xway, seg, dir, pos));
            }
        }
    }
}
