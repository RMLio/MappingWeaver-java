package be.ugent.idlab.knows.flink.operators;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.operators.intermediate.binary.JoinOperator;
import be.ugent.idlab.knows.values.MapTupValue;

// FIXME:  Inefficient nested loop join being done here. Optimize it to use 
// keyed data stream on join attributes<02-09-24, Min Oo> //

public class FlinkJoinOperator
        extends RichCoFlatMapFunction<MapTupValue, MapTupValue, MapTupValue>
        implements CheckpointedFunction {

    private final Logger LOG = LoggerFactory.getLogger(FlinkJoinOperator.class);

    private final JoinOperator joinOperator;

    private transient ListState<MapTupValue> leftState;
    private transient ListState<MapTupValue> rightState;

    private List<MapTupValue> leftBuffer;
    private List<MapTupValue> rightBuffer;

    public FlinkJoinOperator(JoinOperator joinOperator) {
        this.joinOperator = joinOperator;
        this.leftBuffer = new ArrayList<>();
        this.rightBuffer = new ArrayList<>();
    }

    @Override
    public void flatMap1(MapTupValue element, Collector<MapTupValue> out) throws Exception {
        this.leftBuffer.add(element);

        // LOG.warn(String.format("Joining left element: %s \n with right lists: %s",
        // (element.getValue().toString()),
        // StreamSupport.stream(Spliterators.spliteratorUnknownSize(this.rightState.get().iterator(),
        // 0), false)
        // .map(elem -> elem.getValue().toString()).collect(Collectors.toList())
        // .toString()));

        for (MapTupValue rightValue : this.rightBuffer) {
            MappingTuple joinedTuple = this.joinOperator.apply(element.getValue(), rightValue.getValue());
            // LOG.warn(String.format("Resulting joined mapping tuple: \n %s",
            // joinedTuple.toString()));
            out.collect(new MapTupValue(joinedTuple));
        }
    }

    @Override
    public void flatMap2(MapTupValue element, Collector<MapTupValue> out) throws Exception {

        this.rightBuffer.add(element);
        // LOG.warn(String.format("Joining right element: %s \n with left lists: %s",
        // (element.getValue().toString()),
        // StreamSupport.stream(Spliterators.spliteratorUnknownSize(this.leftState.get().iterator(),
        // 0), false)
        // .map(elem -> elem.getValue().toString()).collect(Collectors.toList())
        // .toString()));
        for (MapTupValue leftValue : this.leftBuffer) {
            MappingTuple joinedTuple = this.joinOperator.apply(leftValue.getValue(), element.getValue());
            // LOG.warn(String.format("Resulting joined mapping tuple: \n %s",
            // joinedTuple.toString()));
            out.collect(new MapTupValue(joinedTuple));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext initializationContext) throws Exception {
        ListStateDescriptor<MapTupValue> descriptor1 = new ListStateDescriptor<>("state1",
                TypeInformation.of(new TypeHint<>() {
                }));
        ListStateDescriptor<MapTupValue> descriptor2 = new ListStateDescriptor<>("state2",
                TypeInformation.of(new TypeHint<>() {
                }));

        this.leftState = initializationContext.getOperatorStateStore().getListState(descriptor1);
        this.rightState = initializationContext.getOperatorStateStore().getListState(descriptor2);

        if (initializationContext.isRestored()) {
            for (MapTupValue value : this.leftState.get()) {
                this.leftBuffer.add(value);

            }

            for (MapTupValue value : this.rightState.get()) {
                this.rightBuffer.add(value);

            }

        }

    }

    @Override
    public void snapshotState(FunctionSnapshotContext snapshotContext) throws Exception {
        this.leftState.update(this.leftBuffer);
        this.rightState.update(this.rightBuffer);

    }

}
