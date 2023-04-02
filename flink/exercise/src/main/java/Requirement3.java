import model.UserLoginData;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

public class Requirement3 {
    /*
    需求三：恶意登录监控
    用户在短时间内频繁登录失败，有程序恶意攻击的可能，同一用户（可以是不同 IP）在 2 秒内连续两次登录失败，需要报警。
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

        KeyedStream<UserLoginData, Tuple> keyedStream = executionEnvironment.readTextFile("data/login_data.csv")
                .map(event -> {
                    String[] arr = event.split(",");
                    UserLoginData data = UserLoginData.builder()
                            .ip(arr[0])
                            .username(arr[1])
                            .operateUrl(arr[2])
                            .time(arr[3])
                            .build();
                    if (arr.length > 4) {
                        data.setLoginAttempt(arr[4]);
                    }
                    return data;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserLoginData>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserLoginData>() {
                            @Override
                            public long extractTimestamp(UserLoginData element, long recordTimestamp) {
                                long extractTimestamp = 0;
                                try {
                                    extractTimestamp = fastDateFormat.parse(element.getTime()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                return extractTimestamp;

                            }
                        })
                ).keyBy("username");

        Pattern<UserLoginData, UserLoginData> pattern = Pattern.<UserLoginData>begin("start").where(new SimpleCondition<UserLoginData>() {
                    @Override
                    public boolean filter(UserLoginData login) {
                        return "FAILED".equals(login.getLoginAttempt());
                    }
                })
                .followedByAny("second").where(new SimpleCondition<UserLoginData>() {
                    @Override
                    public boolean filter(UserLoginData login) {
                        return "FAILED".equals(login.getLoginAttempt());
                    }
                })
                .within(Time.seconds(2));

        PatternStream<UserLoginData> deviceDetailPatternStream = CEP.pattern(keyedStream, pattern).inEventTime();

        deviceDetailPatternStream.flatSelect(new PatternFlatSelectFunction<UserLoginData, Void>() {
            @Override
            public void flatSelect(Map<String, List<UserLoginData>> patternMap, Collector<Void> out) throws Exception {

                List<UserLoginData> startMatchList = patternMap.get("start");
                List<UserLoginData> secondMatchList = patternMap.get("second");

                UserLoginData startResult = startMatchList.iterator().next();
                UserLoginData secondResult = secondMatchList.iterator().next();

                System.out.println("Send alert to username=" + startResult.getUsername() + " because detected 2 login fails within 2 seconds:");
                System.out.println("First record: " + startResult);
                System.out.println("Second record: " + secondResult);

            }
        });

        executionEnvironment.execute();
    }
}
