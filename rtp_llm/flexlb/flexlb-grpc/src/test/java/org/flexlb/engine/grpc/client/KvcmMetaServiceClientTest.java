package org.flexlb.engine.grpc.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Deadline;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.engine.grpc.core.GrpcChannelFactory;
import org.flexlb.engine.grpc.core.GrpcTarget;
import org.flexlb.kvcm.grpc.GetClusterInfoRequest;
import org.flexlb.kvcm.grpc.GetClusterInfoResponse;
import org.flexlb.kvcm.grpc.GetHostCacheStateRequest;
import org.flexlb.kvcm.grpc.GetHostCacheStateResponse;
import org.flexlb.kvcm.grpc.MetaServiceGrpc;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KvcmMetaServiceClientTest {

    private static final GrpcTarget LEADER = new GrpcTarget("127.0.0.1", 6381);

    @Test
    void pooledQueriesKeepOtherConnectionResponsiveWhenOneClientEventLoopIsBlocked() throws Exception {
        var received = new LinkedBlockingQueue<PendingReply>();
        var peerAddress = Context.<SocketAddress>key("kvcm-client-address");
        Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                .intercept(new ServerInterceptor() {
                    @Override
                    public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call,
                                                                       Metadata headers,
                                                                       ServerCallHandler<Q, R> next) {
                        var peer = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                        return Contexts.interceptCall(Context.current().withValue(peerAddress, peer),
                                call, headers, next);
                    }
                })
                .addService(new MetaServiceGrpc.MetaServiceImplBase() {
                    @Override
                    public void getHostCacheState(GetHostCacheStateRequest request,
                                                  StreamObserver<GetHostCacheStateResponse> observer) {
                        received.add(new PendingReply(peerAddress.get(), observer));
                    }
                }).build().start();
        var engineLoops = new NioEventLoopGroup(1);
        var kvcmLoops = new NioEventLoopGroup(2);
        var callbacks = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        var configListener = new AtomicReference<Consumer<FlexlbConfig>>();
        var factory = new GrpcChannelFactory(callbacks, engineLoops, kvcmLoops, 2_000, 30_000, false);
        var client = new KvcmMetaServiceClient(factory, configService(configListener));
        try {
            publishPoolSize(configListener.get(), 2);
            var target = new GrpcTarget("127.0.0.1", server.getPort());
            for (EventExecutor eventLoop : kvcmLoops) {
                var first = startQuery(client, target, received);
                var second = startQueryOnOtherConnection(client, target, received, first.reply().peer());
                assertRepliesStayIndependent(eventLoop, first, second);
            }
        } finally {
            client.shutdown();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            kvcmLoops.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            engineLoops.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
            callbacks.shutdownNow();
        }
    }

    @Test
    void separatesClusterInfoFromTheQueryPoolAndResizesOnConfigUpdates() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(new MetaServiceGrpc.MetaServiceImplBase() {
                    @Override
                    public void getClusterInfo(GetClusterInfoRequest request,
                                               StreamObserver<GetClusterInfoResponse> observer) {
                        observer.onNext(GetClusterInfoResponse.getDefaultInstance());
                        observer.onCompleted();
                    }

                    @Override
                    public void getHostCacheState(GetHostCacheStateRequest request,
                                                  StreamObserver<GetHostCacheStateResponse> observer) {
                        observer.onNext(GetHostCacheStateResponse.getDefaultInstance());
                        observer.onCompleted();
                    }
                }).build().start();
        List<ManagedChannel> channels = new ArrayList<>();
        var configListener = new AtomicReference<Consumer<FlexlbConfig>>();
        KvcmMetaServiceClient client = createClient(serverName, channels, configListener);
        try {
            client.getClusterInfo(LEADER, GetClusterInfoRequest.getDefaultInstance(), 2_000);
            assertEquals(1, channels.size());

            runQueries(client);
            client.getClusterInfo(LEADER, GetClusterInfoRequest.getDefaultInstance(), 2_000);
            assertEquals(9, channels.size());

            publishPoolSize(configListener.get(), 1);
            assertEquals(2, channels.stream().filter(channel -> !channel.isShutdown()).count());
            runQueries(client);
            assertEquals(9, channels.size());

            publishPoolSize(configListener.get(), 3);
            runQueries(client);
            assertEquals(11, channels.size());
            assertEquals(4, channels.stream().filter(channel -> !channel.isShutdown()).count());

            GrpcTarget newTarget = new GrpcTarget("127.0.0.2", 6381);
            client.removeStaleQueryChannels(newTarget);
            assertFalse(channels.getFirst().isShutdown());
            assertTrue(channels.subList(1, 11).stream().allMatch(ManagedChannel::isShutdown));
            var query = client.getHostCacheStateAsync(newTarget, GetHostCacheStateRequest.getDefaultInstance(),
                    Deadline.after(2, TimeUnit.SECONDS));
            assertEquals(GetHostCacheStateResponse.getDefaultInstance(), query.get(2, TimeUnit.SECONDS));
            assertEquals(12, channels.size());

            client.getClusterInfo(newTarget, GetClusterInfoRequest.getDefaultInstance(), 2_000);
            assertEquals(13, channels.size());
            assertTrue(channels.getFirst().isShutdown());
            assertFalse(channels.getLast().isShutdown());
        } finally {
            client.shutdown();
            server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
        assertTrue(channels.stream().allMatch(ManagedChannel::isShutdown));
    }

    @Test
    void cancellingFutureCancelsTheRpc() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(new MetaServiceGrpc.MetaServiceImplBase() {
                    @Override
                    public void getHostCacheState(GetHostCacheStateRequest request,
                                                  StreamObserver<GetHostCacheStateResponse> observer) {
                        ((ServerCallStreamObserver<?>) observer).setOnCancelHandler(cancelled::countDown);
                        started.countDown();
                    }
                }).build().start();
        KvcmMetaServiceClient client = createClient(serverName, new ArrayList<>(), new AtomicReference<>());
        try {
            var call = client.getHostCacheStateAsync(LEADER, GetHostCacheStateRequest.getDefaultInstance(),
                    Deadline.after(10, TimeUnit.SECONDS));
            assertTrue(started.await(2, TimeUnit.SECONDS));
            assertFalse(call.isDone());
            assertTrue(call.cancel(true));
            assertTrue(cancelled.await(2, TimeUnit.SECONDS));
        } finally {
            client.shutdown();
            server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private static StartedQuery startQuery(KvcmMetaServiceClient client,
                                           GrpcTarget target,
                                           BlockingQueue<PendingReply> received) throws InterruptedException {
        var future = client.getHostCacheStateAsync(target, GetHostCacheStateRequest.getDefaultInstance(),
                Deadline.after(30, TimeUnit.SECONDS));
        var reply = received.poll(5, TimeUnit.SECONDS);
        assertNotNull(reply, "KVCM should receive the RPC over Netty");
        assertNotNull(reply.peer(), "KVCM should see the client's TCP address");
        return new StartedQuery(future, reply);
    }

    private static StartedQuery startQueryOnOtherConnection(KvcmMetaServiceClient client,
                                                            GrpcTarget target,
                                                            BlockingQueue<PendingReply> received,
                                                            SocketAddress firstPeer) throws Exception {
        for (int attempt = 0; attempt < 64; attempt++) {
            var query = startQuery(client, target, received);
            if (!firstPeer.equals(query.reply().peer())) {
                return query;
            }
            query.reply().send();
            query.future().get(5, TimeUnit.SECONDS);
        }
        throw new AssertionError("The KVCM pool should use a second TCP connection");
    }

    private static void assertRepliesStayIndependent(EventExecutor eventLoop,
                                                     StartedQuery first,
                                                     StartedQuery second) throws Exception {
        CountDownLatch loopBlocked = new CountDownLatch(1);
        CountDownLatch releaseLoop = new CountDownLatch(1);
        CountDownLatch responseReceived = new CountDownLatch(1);
        first.future().addListener(responseReceived::countDown, Runnable::run);
        second.future().addListener(responseReceived::countDown, Runnable::run);
        eventLoop.execute(() -> {
            loopBlocked.countDown();
            try {
                releaseLoop.await();
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            assertTrue(loopBlocked.await(5, TimeUnit.SECONDS), "The client EventLoop should be blocked");
            first.reply().send();
            second.reply().send();
            assertTrue(responseReceived.await(5, TimeUnit.SECONDS),
                    "The other KVCM connection should receive its reply while this EventLoop is blocked");
            var completed = first.future().isDone() ? first : second;
            var blocked = completed == first ? second : first;
            assertEquals(GetHostCacheStateResponse.getDefaultInstance(), completed.future().get());
            assertFalse(blocked.future().isDone(), "The reply on the blocked EventLoop should still be pending");
        } finally {
            releaseLoop.countDown();
        }
        assertEquals(GetHostCacheStateResponse.getDefaultInstance(), first.future().get(5, TimeUnit.SECONDS));
        assertEquals(GetHostCacheStateResponse.getDefaultInstance(), second.future().get(5, TimeUnit.SECONDS));
    }

    private record PendingReply(SocketAddress peer, StreamObserver<GetHostCacheStateResponse> observer) {
        void send() {
            observer.onNext(GetHostCacheStateResponse.getDefaultInstance());
            observer.onCompleted();
        }
    }

    private record StartedQuery(ListenableFuture<GetHostCacheStateResponse> future, PendingReply reply) {
    }

    private static void runQueries(KvcmMetaServiceClient client) throws Exception {
        for (int i = 0; i < 256; i++) {
            var query = client.getHostCacheStateAsync(LEADER, GetHostCacheStateRequest.getDefaultInstance(),
                    Deadline.after(2, TimeUnit.SECONDS));
            assertEquals(GetHostCacheStateResponse.getDefaultInstance(), query.get(2, TimeUnit.SECONDS));
        }
    }

    private static void publishPoolSize(Consumer<FlexlbConfig> listener, int size) {
        var config = new FlexlbConfig();
        config.getKvcmAndStandbyConfig().setQueryChannelsPerLeader(size);
        listener.accept(config);
    }

    private static KvcmMetaServiceClient createClient(String serverName,
                                                      List<ManagedChannel> channels,
                                                      AtomicReference<Consumer<FlexlbConfig>> configListener) {
        GrpcChannelFactory factory = mock(GrpcChannelFactory.class);
        when(factory.createKvcmChannel(any())).thenAnswer(ignored -> {
            ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
            channels.add(channel);
            return channel;
        });
        return new KvcmMetaServiceClient(factory, configService(configListener));
    }

    private static ConfigService configService(AtomicReference<Consumer<FlexlbConfig>> configListener) {
        ConfigService configService = mock(ConfigService.class);
        doAnswer(invocation -> {
            Consumer<FlexlbConfig> listener = invocation.getArgument(0);
            configListener.set(listener);
            listener.accept(new FlexlbConfig());
            return null;
        }).when(configService).addUpdateListener(any());
        return configService;
    }
}
