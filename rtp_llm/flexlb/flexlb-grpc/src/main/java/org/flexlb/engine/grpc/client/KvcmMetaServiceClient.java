package org.flexlb.engine.grpc.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.KvcmCacheMatchingConfig;
import org.flexlb.engine.grpc.core.GrpcChannelFactory;
import org.flexlb.engine.grpc.core.GrpcChannelPool;
import org.flexlb.engine.grpc.core.GrpcTarget;
import org.flexlb.kvcm.grpc.GetClusterInfoRequest;
import org.flexlb.kvcm.grpc.GetClusterInfoResponse;
import org.flexlb.kvcm.grpc.GetHostCacheStateRequest;
import org.flexlb.kvcm.grpc.GetHostCacheStateResponse;
import org.flexlb.kvcm.grpc.MetaServiceGrpc;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Uses separate connections for KVCM leader discovery and cache queries.
 */
@Component
public class KvcmMetaServiceClient {

    private final GrpcChannelPool<GrpcTarget> clusterInfoChannels;
    private final GrpcChannelPool<QueryChannelKey> cacheQueryChannels;
    private volatile int queryChannelsPerLeader = KvcmCacheMatchingConfig.DEFAULT_QUERY_CHANNELS_PER_LEADER;

    public KvcmMetaServiceClient(GrpcChannelFactory channelFactory, ConfigService configService) {
        this.clusterInfoChannels = new GrpcChannelPool<>(channelFactory::createKvcmChannel);
        this.cacheQueryChannels = new GrpcChannelPool<>(key -> channelFactory.createKvcmChannel(key.target()));
        configService.addUpdateListener(this::updateQueryPoolSize);
    }

    public GetClusterInfoResponse getClusterInfo(
            GrpcTarget target,
            GetClusterInfoRequest request,
            long requestTimeoutMs) {
        return MetaServiceGrpc.newBlockingStub(clusterInfoChannel(target))
                .withDeadlineAfter(requestTimeoutMs, TimeUnit.MILLISECONDS)
                .getClusterInfo(request);
    }

    public ListenableFuture<GetHostCacheStateResponse> getHostCacheStateAsync(GrpcTarget target,
                                                                             GetHostCacheStateRequest request,
                                                                             Deadline queryDeadline) {
        int slot = ThreadLocalRandom.current().nextInt(queryChannelsPerLeader);
        var channel = cacheQueryChannels.getOrCreate(new QueryChannelKey(target, slot)).getChannel();
        return MetaServiceGrpc.newFutureStub(channel).withDeadline(queryDeadline).getHostCacheState(request);
    }

    void removeStaleQueryChannels(GrpcTarget leader) {
        cacheQueryChannels.removeChannelsForInactiveGroups(Set.of(true),
                key -> key.target().equals(leader) && key.slot() < queryChannelsPerLeader);
    }

    @PreDestroy
    public void shutdown() {
        clusterInfoChannels.shutdown();
        cacheQueryChannels.shutdown();
    }

    private ManagedChannel clusterInfoChannel(GrpcTarget target) {
        clusterInfoChannels.removeStaleChannels(Set.of(target));
        return clusterInfoChannels.getOrCreate(target).getChannel();
    }

    private void updateQueryPoolSize(FlexlbConfig config) {
        if (!config.isKvcmAndStandbyEnabled()) {
            return;
        }
        int previousSize = queryChannelsPerLeader;
        int newSize = config.getKvcmAndStandbyConfig().getQueryChannelsPerLeader();
        queryChannelsPerLeader = newSize;
        if (newSize < previousSize) {
            cacheQueryChannels.removeChannelsForInactiveGroups(Set.of(true), key -> key.slot() < newSize);
        }
    }

    private record QueryChannelKey(GrpcTarget target, int slot) {
    }
}
