package com.example.tpch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TPCHFullExperiment {
    private static final String DATA_PATH = "D:/Users/Josepha/Desktop/Data1mb/";

    // Parameters
    private static final int PARALLELISM = 4;
    private static final int BATCH_SIZE = 2000;
    private static final int DELETE_COUNT = 100;
    private static final long RANDOM_SEED = 999L;

    public static final ConcurrentHashMap<String, Double> flinkResults = new ConcurrentHashMap<>();
    private static final int SAMPLE_LIMIT = 5;

    public static class OrderEvent implements Serializable {
        public long orderKey; public String priority; public boolean isDelete;
        public OrderEvent() {}
        public OrderEvent(long k, String p, boolean d) { this.orderKey=k; this.priority=p; this.isDelete=d; }
    }
    public static class LineItem implements Serializable {
        public long orderKey; public double extendedPrice;
        public LineItem() {}
        public LineItem(long k, double p) { this.orderKey=k; this.extendedPrice=p; }
    }
    public static class JoinedRow implements Serializable {
        public String priority; public double price; public boolean isRetract;
        public JoinedRow() {}
        public JoinedRow(String p, double pr, boolean r) { this.priority=p; this.price=pr; this.isRetract=r; }
    }


    public static void main(String[] args) throws Exception {

        System.out.println("TPC-H Experiment");
        System.out.println("===============================================================");
        System.out.printf("Parameters: Parallelism=%d | Batch=%d | Delete=%d | Seed=%d%n",
                PARALLELISM, BATCH_SIZE, DELETE_COUNT, RANDOM_SEED);

        System.out.println("\n[Phase 1] Generated Ground Truth...");
        long startGt = System.nanoTime();

        List<OrderEvent> orderEvents = generateOrderEvents(DATA_PATH + "orders.tbl", BATCH_SIZE, DELETE_COUNT, RANDOM_SEED);
        List<LineItem> lineItems = generateLineItems(DATA_PATH + "lineitem.tbl");
        // **Calculate Ground Truth**
        Map<String, Double> groundTruth = computeGroundTruthLocal(orderEvents, lineItems);
        long endGt = System.nanoTime();

        System.out.printf("The data sequence has been generated successfully (Order Events: %d, Line Items: %d)，耗时: %.2f ms%n",
                orderEvents.size(), lineItems.size(), (endGt - startGt) / 1_000_000.0);

        // Parse2
        System.out.println("\n[Phase 2] Start Flink...");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(PARALLELISM);

        // Create DataStream
        DataStream<OrderEvent> orderStream = env
                .fromCollection(orderEvents)
                .name("Source-Orders");

        DataStream<LineItem> lineItemStream = env
                .fromCollection(lineItems)
                .name("Source-LineItem");

        DataStream<JoinedRow> joinedStream = orderStream
                .keyBy(o -> o.orderKey)
                .connect(lineItemStream.keyBy(l -> l.orderKey))
                .process(new CascadeJoinFunction())
                .name("Process-Join-Cascade");

        DataStream<Tuple2<String, Double>> aggResult = joinedStream
                .keyBy(r -> r.priority)
                .process(new AggregationFunction())
                .name("Process-Aggregation");

        aggResult.addSink(new ResultCollectingSink()).setParallelism(1);

        long startFlink = System.nanoTime();
        try {
            env.execute("TPC-H Cascade Experiment");
        } catch (Exception e) {
            System.err.println("\n Wrong!Wrong!Wrong!: " + e.getMessage());
            e.printStackTrace();
            return;
        }
        long endFlink = System.nanoTime();

        //Phase3 Result
        printPerformanceReport(startFlink, endFlink, orderEvents.size());
        verifyCorrectness(groundTruth, flinkResults);
    }

    private static List<OrderEvent> generateOrderEvents(String path, int batchSize, int deleteCount, long seed) throws Exception {
        List<OrderEvent> events = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;
        List<Long> currentBatchKeys = new ArrayList<>();
        int insertSampleCount = 0;
        int deleteTotalCount = 0;
        Random rand = new Random(seed);
        int totalOrderRows = 0;

        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\\|");
            long orderKey = Long.parseLong(parts[0]);
            String priority = parts[5];
            totalOrderRows++;

            // 1.INSERT
            OrderEvent insertEvent = new OrderEvent(orderKey, priority, false);
            events.add(insertEvent);
            currentBatchKeys.add(orderKey);
            // Showing insert information
            if (insertSampleCount < SAMPLE_LIMIT) {
                System.out.printf("  [INSERT 样本 %d] OrderKey: %d, Priority: %s%n", insertSampleCount + 1, orderKey, priority);
                insertSampleCount++;
            }

            // 2. DELETE Logic
            if (currentBatchKeys.size() >= batchSize) {
                for (int i = 0; i < deleteCount; i++) {
                    if (currentBatchKeys.isEmpty()) break;
                    int idx = rand.nextInt(currentBatchKeys.size());
                    Long keyToDelete = currentBatchKeys.remove(idx);

                    // DELETE Event
                    events.add(new OrderEvent(keyToDelete, null, true));

                    if (deleteTotalCount < SAMPLE_LIMIT) {
                        System.out.printf("  [DELETE Item %d] OrderKey: %d%n", deleteTotalCount + 1, keyToDelete);
                    }
                    deleteTotalCount++;
                }
                currentBatchKeys.clear();
            }
        }
        br.close();
        System.out.printf("\nOrder Row: %d, Total Delete operations: %d%n", totalOrderRows, deleteTotalCount);

        return events;
    }

    private static List<LineItem> generateLineItems(String path) throws Exception {
        List<LineItem> items = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\\|");
            long orderKey = Long.parseLong(parts[0]);
            double price = Double.parseDouble(parts[5]);
            items.add(new LineItem(orderKey, price));
        }
        br.close();
        return items;
    }

    public static class CascadeJoinFunction extends KeyedCoProcessFunction<Long, OrderEvent, LineItem, JoinedRow> {
        private transient ValueState<String> orderPriorityState;
        private transient MapState<String, Double> lineItemsState;

        @Override
        public void open(Configuration parameters) {
            orderPriorityState = getRuntimeContext().getState(new ValueStateDescriptor<>("priority", String.class));
            lineItemsState = getRuntimeContext().getMapState(new MapStateDescriptor<>("lines", String.class, Double.class));
        }

        @Override
        public void processElement1(OrderEvent order, Context ctx, Collector<JoinedRow> out) throws Exception {
            if (!order.isDelete) {
                orderPriorityState.update(order.priority);
                if (lineItemsState.keys() != null) {
                    for (Map.Entry<String, Double> entry : lineItemsState.entries()) {
                        out.collect(new JoinedRow(order.priority, entry.getValue(), false));
                    }
                }
            } else {
                String priority = orderPriorityState.value();
                if (priority != null) {
                    if (lineItemsState.keys() != null) {
                        for (Map.Entry<String, Double> entry : lineItemsState.entries()) {
                            out.collect(new JoinedRow(priority, entry.getValue(), true));
                        }
                    }
                    orderPriorityState.clear();
                    lineItemsState.clear();
                }
            }
        }

        @Override
        public void processElement2(LineItem item, Context ctx, Collector<JoinedRow> out) throws Exception {
            lineItemsState.put(UUID.randomUUID().toString(), item.extendedPrice);
            String priority = orderPriorityState.value();
            if (priority != null) {
                out.collect(new JoinedRow(priority, item.extendedPrice, false));
            }
        }
    }

    public static class AggregationFunction extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<String, JoinedRow, Tuple2<String, Double>> {
        private transient ValueState<Double> sumState;

        @Override
        public void open(Configuration parameters) {
            sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Double.class));
        }

        @Override
        public void processElement(JoinedRow row, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
            Double currentSum = sumState.value();
            if (currentSum == null) currentSum = 0.0;

            if (row.isRetract) {
                currentSum -= row.price;
            } else {
                currentSum += row.price;
            }
            sumState.update(currentSum);

            out.collect(Tuple2.of(ctx.getCurrentKey(), currentSum));
        }
    }

    public static class ResultCollectingSink implements SinkFunction<Tuple2<String, Double>> {
        @Override
        public void invoke(Tuple2<String, Double> value, Context context) {
            flinkResults.put(value.f0, value.f1);
        }
    }

    private static Map<String, Double> computeGroundTruthLocal(List<OrderEvent> orderEvents, List<LineItem> lineItems) {
        Map<Long, String> finalOrders = new HashMap<>();
        Map<Long, List<Double>> initialLineItems = new HashMap<>();

        for (LineItem item : lineItems) {
            initialLineItems.computeIfAbsent(item.orderKey, k -> new ArrayList<>()).add(item.extendedPrice);
        }

        for (OrderEvent event : orderEvents) {
            if (!event.isDelete) {
                finalOrders.put(event.orderKey, event.priority);
            } else {
                finalOrders.remove(event.orderKey);
            }
        }

        Map<String, Double> result = new HashMap<>();
        for (Map.Entry<Long, String> entry : finalOrders.entrySet()) {
            Long orderKey = entry.getKey();
            String priority = entry.getValue();
            if (initialLineItems.containsKey(orderKey)) {
                for (Double price : initialLineItems.get(orderKey)) {
                    result.put(priority, result.getOrDefault(priority, 0.0) + price);
                }
            }
        }
        return result;
    }

    private static void printPerformanceReport(long startNano, long endNano, int totalOps) {
        double durationSeconds = (endNano - startNano) / 1_000_000_000.0;

        System.out.println("\nPerformance");
        System.out.printf("Time: %.2f s%n", durationSeconds);
        System.out.printf("Event: %d 条%n", totalOps);
        System.out.printf("Avg.: %.2f Ops/sec%n", totalOps / durationSeconds);
        System.out.println("=======================");
    }

    private static void verifyCorrectness(Map<String, Double> expected, Map<String, Double> actual) {
        System.out.println("\nCorrectness Check");
        boolean allMatch = true;

        List<String> keys = new ArrayList<>(expected.keySet());
        Collections.sort(keys);

        System.out.printf("%-20s | %-15s | %-15s | %-10s%n", "Priority", "Expected", "Flink Actual", "Status");
        System.out.println("----------------------------------------------------------------------");

        for (String key : keys) {
            double expVal = expected.get(key);
            double actVal = actual.getOrDefault(key, 0.0);
            double diff = Math.abs(expVal - actVal);

            boolean match = diff < 0.01;

            if (!match) allMatch = false;

            System.out.printf("%-20s | %-15.2f | %-15.2f | %s%n",
                    key, expVal, actVal, match ? "PASS" : "FAIL");
        }
        System.out.println("----------------------------------------------------------------------");

        if (allMatch) {
            System.out.println("Final Results: SUCCESS");
        } else {
            System.err.println("Final Results: FAILURE");
        }
    }
}