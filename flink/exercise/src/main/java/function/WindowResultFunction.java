package function;

import model.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFunction<T> implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

    @Override
    public void apply(
            Tuple key,  // 窗口的主键，即 itemId
            TimeWindow window,  // 窗口
            Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
            Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
    ) {
        T itemId = ((Tuple1<T>) key).f0;
        Long count = aggregateResult.iterator().next();
        collector.collect(new ItemViewCount(String.valueOf(itemId), window.getEnd(), count));
    }
}