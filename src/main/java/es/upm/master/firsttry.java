package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class firsttry {

    public static void main(String[] args)  throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String	inFilePath = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\traffic-3xways";
        String	outFilePath = "C:\\Users\\Goncalo\\IdeaProjects\\flinkPr\\projectFlink\\output";
        System.out.println("Started to read the file...");
        DataStreamSource<String> source	= env.readTextFile(inFilePath).setParallelism(1);


        SingleOutputStreamOperator<String> speedRadar = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                String[] s = value.split(",");
                int speed = Integer.parseInt(s[2]);
                return speed > 90;
            }
        });

        SingleOutputStreamOperator<Tuple8<String, String, String, String, String, String, String, String>> mappedRadar = speedRadar.map(new MapFunction<String, Tuple8<String,String,String,String,String,String,String,String>>() {
            @Override
            public Tuple8<String,String,String,String,String,String,String,String> map(String value) {
                String[] s = value.split(",");
                return new Tuple8<>(s[0],s[1],s[2],s[3],s[4],s[5],s[6],s[7]);
            }
        });

        mappedRadar.writeAsCsv(outFilePath);
        env.execute();
    }
}
