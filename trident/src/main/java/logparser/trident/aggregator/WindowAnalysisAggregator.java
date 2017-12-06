package logparser.trident.aggregator;

import org.apache.storm.tuple.Values;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;


public class WindowAnalysisAggregator extends BaseAggregator<WindowAnalysisAggregator.State> {

    static class State {
        long sum = 0;
    }
    
    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
        state.sum += tuple.getInteger(0);
    }

    @Override
    public void complete(State state, TridentCollector collector) {
        collector.emit(new Values(state.sum));
    }
    
}
