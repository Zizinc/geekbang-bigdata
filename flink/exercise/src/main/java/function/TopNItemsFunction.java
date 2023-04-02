package function;

import model.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopNItemsFunction extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
    private final int topSize;

    public TopNItemsFunction(int topSize) {
        this.topSize = topSize;
    }

    private ListState<ItemViewCount> itemState;

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        ListStateDescriptor<ItemViewCount> itemStateDesc = new ListStateDescriptor<ItemViewCount>(
                "itemState-State",
                ItemViewCount.class
        );
        itemState = getRuntimeContext().getListState(itemStateDesc);
    }

    @Override
    public void processElement(ItemViewCount itemViewCount, KeyedProcessFunction<Tuple, ItemViewCount, String>.Context context, Collector<String> collector) throws Exception {
        itemState.add(itemViewCount);
        context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<String> out) throws Exception {
        List<ItemViewCount> allItems = new ArrayList<>();
        for (ItemViewCount item: itemState.get()) {
            allItems.add(item);
        }
        itemState.clear();

        allItems.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int) (o2.getViewCount() - o1.getViewCount());
            }
        });

        StringBuilder sb = new StringBuilder();
        sb.append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < Math.min(topSize, allItems.size()); i++) {
            ItemViewCount item = allItems.get(i);
            sb.append("No." + (i+1) + ": itemId=" + item.getItemId() + ",viewCount=" + item.getViewCount() + "\n");
        }
        out.collect(sb.toString());
    }
}