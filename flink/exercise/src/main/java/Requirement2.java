import function.CountAggregateFunction;
import function.TopNItemsFunction;
import function.WindowResultFunction;
import model.ItemViewCount;
import model.LogData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Requirement2 {
    /*
    需求二：页面浏览数统计
    即统计在一段时间内用户访问某个 url 的次数，输出某段时间内访问量最多的前 N 个 URL。
    如每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL。
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<LogData> log = executionEnvironment.readTextFile("data/log_data.csv")
                .map(event -> {
                    String[] arr = event.split(",");
                    return LogData.builder()
                            .ip(arr[0])
                            .userId(arr[1])
                            .eventTime(Long.parseLong(arr[2]))
                            .method(arr[3])
                            .url(arr[4])
                            .build();
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LogData>() {
                    @Override
                    public long extractAscendingTimestamp(LogData logData) {
                        return logData.getEventTime() * 1000;
                    }
                });
        DataStream<ItemViewCount> aggWindowDS = log.keyBy("url")
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(new CountAggregateFunction(), new WindowResultFunction());

        aggWindowDS.keyBy("windowEnd")
                .process(new TopNItemsFunction(3))
                .print();

        executionEnvironment.execute();
    }
}
