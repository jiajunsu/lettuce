package io.lettuce.core.masterreplica;

import static io.lettuce.core.masterreplica.ReplicaUtils.findNodeByUri;
import static io.lettuce.core.masterreplica.TopologyComparators.LatencyComparator;

import java.util.*;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import io.lettuce.core.RedisURI;
import io.lettuce.core.masterreplica.TopologyComparators.SortAction;
import io.lettuce.core.models.role.RedisNodeDescription;

/**
 * Encapsulates asynchronously executed commands to multiple {@link RedisURI nodes}.
 *
 * @author Mark Paluch
 */
class Requests extends
        CompletableEventLatchSupport<Tuple2<RedisURI, TimedAsyncCommand<String, String, String>>, List<RedisNodeDescription>> {

    private final Map<RedisURI, TimedAsyncCommand<String, String, String>> rawViews = new TreeMap<>(
            ReplicaUtils.RedisURIComparator.INSTANCE);

    private final List<RedisNodeDescription> nodes;

    private static final InternalLogger LOG = InternalLoggerFactory.getInstance(Requests.class);

    private boolean shouldFail = false;

    public Requests(int expectedCount, List<RedisNodeDescription> nodes) {
        super(expectedCount);
        this.nodes = nodes;
    }

    protected void addRequest(RedisURI redisURI, TimedAsyncCommand<String, String, String> command) {

        rawViews.put(redisURI, command);
        command.onComplete((s, throwable) -> {

            if (throwable != null) {
                accept(throwable);
            } else {
                accept(Tuples.of(redisURI, command));
            }
        });
    }

    @Override
    protected void onEmit(Emission<List<RedisNodeDescription>> emission) {

        List<RedisNodeDescription> result = new ArrayList<>();

        Map<RedisNodeDescription, Long> latencies = new HashMap<>();

        for (RedisNodeDescription node : nodes) {

            TimedAsyncCommand<String, String, String> future = getRequest(node.getUri());

            if (shouldFail) {
                LOG.error("TEST: Requests node " + node.getUri() + " set future to null ");
                future = null;
            }

            if (future == null || !future.isDone()) {
                continue;
            }

            RedisNodeDescription redisNodeDescription = findNodeByUri(nodes, node.getUri());
            latencies.put(redisNodeDescription, future.duration());
            result.add(redisNodeDescription);
        }

        SortAction sortAction = SortAction.getSortAction();
        sortAction.sort(result, new LatencyComparator(latencies));

        emission.success(result);
    }

    protected Set<RedisURI> nodes() {
        return rawViews.keySet();
    }

    protected TimedAsyncCommand<String, String, String> getRequest(RedisURI redisURI) {
        return rawViews.get(redisURI);
    }

    public void shouldFail() {
        this.shouldFail = true;
    }

}
