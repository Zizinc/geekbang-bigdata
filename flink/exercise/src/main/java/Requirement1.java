import function.CountAggregateFunction;
import function.TopNItemsFunction;
import function.WindowResultFunction;
import model.ItemViewCount;
import model.UserBehaviorData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Requirement1 {
    /*
    需求一 热门商品 PV 统计
    问题： 每隔 5 分钟输出最近一小时内点击量最多的前 N 个商品，热门度点击量用浏览次数（“pv”）来衡量
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<UserBehaviorData> userBehavior = executionEnvironment.readTextFile("data/user_behavior_data.csv")
                .map(event -> {
                    String[] arr = event.split(",");
                    return UserBehaviorData.builder()
                            .userId(Long.parseLong(arr[0]))
                            .itemId(Long.parseLong(arr[1]))
                            .categoryId(Long.parseLong(arr[2]))
                            .behavior(arr[3])
                            .timestamp(Long.parseLong(arr[4]))
                            .build();
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehaviorData>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehaviorData userBehaviorData) {
                        return userBehaviorData.getTimestamp() * 1000;
                    }
                });

        DataStream<ItemViewCount> aggWindowDS = userBehavior.filter(event -> event.getBehavior().equals("pv"))
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAggregateFunction(), new WindowResultFunction());

        aggWindowDS.keyBy("windowEnd")
                .process(new TopNItemsFunction(3))
                .print();

        executionEnvironment.execute();
    }
}
