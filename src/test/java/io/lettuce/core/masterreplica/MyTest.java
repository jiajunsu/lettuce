package io.lettuce.core.masterreplica;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.test.ReflectionTestUtils;
import io.lettuce.test.resource.TestClientResources;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import reactor.core.publisher.Mono;

/**
 * @author Victor
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MyTest {

    private static final InternalLogger LOG = InternalLoggerFactory.getInstance(MyTest.class);

    @Test
    void refreshEmptyNodes() throws Exception {
        RedisURI uri = RedisURI
                .create("redis-sentinel://127.0.0.1:26379,127.0.0.1:26380?sentinelMasterId=mymaster&timeout=10s");
        RedisClient redisClient = RedisClient.create(TestClientResources.get(), uri);

        SentinelConnector<String, String> connector = new SentinelConnector<>(redisClient, StringCodec.UTF8, uri);

        // Trigger refresh, sentinelTopologyRefresh and connectionProvider initializing
        StatefulRedisMasterReplicaConnection<String, String> connection = connector.connectAsync().get();

        SentinelTopologyRefresh sentinelTopologyRefresh = connector.sentinelTopologyRefresh;
        SentinelTopologyRefresh.PubSubMessageHandler adapter = ReflectionTestUtils.getField(sentinelTopologyRefresh,
                "messageHandler");

        MasterReplicaConnectionProvider<String, String> connectionProvider = connector.connectionProvider;

        // Publish TopologyRefreshMessage
        LOG.error("TEST: +sdown 1");
        adapter.handle("*", "+sdown", "sentinel 127.0.0.1:26379 127.0.0.1 26379 @ mymaster 127.0.0.1 6482");
        Mono.delay(Duration.ofMillis(10000)).block();
        LOG.error("TEST: +sdown 2");
        adapter.handle("*", "+sdown", "slave 127.0.0.1:6483 127.0.0.1 6483 @ mymaster 127.0.0.1 6482");
        Mono.delay(Duration.ofMillis(200)).block();
        LOG.error("TEST: +sdown 3");
        adapter.handle("*", "+sdown", "sentinel 127.0.0.1:26379 127.0.0.1 26379 @ mymaster 127.0.0.1 6482");

        Mono.delay(Duration.ofSeconds(10)).block();

        LOG.error("TEST: known nodes " + connectionProvider.knownNodes);
        // Should not fail here
        LOG.error("TEST: master node " + connectionProvider.getMaster());
    }

}
