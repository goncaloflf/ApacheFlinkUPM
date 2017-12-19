package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

public class firsttry {

    public static void main(String[] args)  throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        String	inFilePath = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\traffic-3xways";
        String	outFilePath = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\output.txt";
        System.out.println("Started to read the file...");
        DataStream<String> source = env.readTextFile(inFilePath).setParallelism(1);


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

        speedRadar.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }
}
