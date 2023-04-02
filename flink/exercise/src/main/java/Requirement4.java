import model.OrderData;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

public class Requirement4 {
    /*
    需求四：订单支付实时监控
    最终创造收入和利润的是用户下单购买的环节；更具体一点，是用户真正完成支付动作的时候。用户下单的行为可以表明用户对商品的需求，
    但在现实中，并不是每次下单都会被用户立刻支付。当拖延一段时间后，用户支付的意愿会降低，并且为了降低安全风险，电商网站往往会对订单状态进行监控。
    设置一个失效时间（比如 15 分钟），如果下单后一段时间仍未支付，订单就会被取消。
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

        KeyedStream<OrderData, Tuple> keyedStream = executionEnvironment.readTextFile("data/order_data.csv")
                .map(event -> {
                    String[] arr = event.split(",");
                    return OrderData.builder()
                            .orderId(arr[0])
                            .status(arr[1])
                            .orderCreateTime(arr[2])
                            .price(Double.parseDouble(arr[3]))
                            .build();
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderData>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderData>() {
                            @Override
                            public long extractTimestamp(OrderData element, long recordTimestamp) {
                                long extractTimestamp = 0;
                                try {
                                    extractTimestamp = fastDateFormat.parse(element.getOrderCreateTime()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                return extractTimestamp;

                            }
                        })
                ).keyBy("orderId");

        Pattern<OrderData, OrderData> pattern = Pattern.<OrderData>begin("start").where(new SimpleCondition<OrderData>() {
                    @Override
                    public boolean filter(OrderData orderData) {
                        return orderData.getStatus().equals("1");
                    }
                })
                .followedBy("second").where(new SimpleCondition<OrderData>() {
                    @Override
                    public boolean filter(OrderData orderData) {
                        return orderData.getStatus().equals("2");
                    }
                }).within(Time.minutes(15));

        PatternStream<OrderData> patternStream = CEP.pattern(keyedStream, pattern).inEventTime();
        OutputTag outputTag = new OutputTag("timeout", TypeInformation.of(OrderData.class));

        SingleOutputStreamOperator<OrderData> result = patternStream.select(outputTag, new OrderPatternTimeoutFunction(), new OrderPatternSelectFunction());

        result.getSideOutput(outputTag).map(obj -> {
            OrderData orderData = (OrderData) obj;
            System.out.println("orderId=" + orderData.getOrderId() + " will be cancelled because it did not receive payment within 15 mins: " + orderData);
            return orderData;
        });

        executionEnvironment.execute();
    }
    static class OrderPatternTimeoutFunction implements PatternTimeoutFunction<OrderData, OrderData> {
        @Override
        public OrderData timeout(Map<String, List<OrderData>> patternMap, long timeoutTimestamp) {
            List<OrderData> startTimeoutOrderDetails = patternMap.get("start");
            OrderData orderDetailTimeout = startTimeoutOrderDetails.iterator().next();
            return orderDetailTimeout;
        }
    }

    static class OrderPatternSelectFunction implements PatternSelectFunction<OrderData, OrderData> {
        @Override
        public OrderData select(Map<String, List<OrderData>> patternMap) {
            List<OrderData> secondOrderDetails = patternMap.get("second");
            OrderData orderDetailSuccess = secondOrderDetails.iterator().next();
            return orderDetailSuccess;
        }
    }
}
