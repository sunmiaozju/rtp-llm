package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.route.RoleType;

import java.util.Objects;

/**
 * One exact endpoint-generation selection.
 *
 * <p>The selection owns its endpoint hold until the router moves it into a
 * queue-route admission.  It carries only immutable
 * routing output besides that hold; {@link ServerStatus} remains response
 * metadata and is never an ownership token.</p>
 */
public final class SelectedRole implements AutoCloseable {

    private WorkerEndpoint.EndpointHold endpointHold;
    private final ServerStatus serverStatus;
    private final long placementVersion;
    private SelectedRole(WorkerEndpoint.EndpointHold endpointHold,
                         ServerStatus serverStatus,
                         long placementVersion) {
        this.endpointHold = endpointHold;
        this.serverStatus = serverStatus;
        if (!serverStatus.isSuccess()) {
            throw new IllegalArgumentException(
                    "SelectedRole requires successful response metadata");
        }
        WorkerEndpoint endpoint = endpointHold.endpoint();
        if (endpoint.getStatus().getGenerationId() != endpointHold.generationId()) {
            throw new IllegalArgumentException("selection hold does not match endpoint generation");
        }
        if (!Objects.equals(serverStatus.getServerIp(), endpoint.getIp())
                || serverStatus.getHttpPort() != endpoint.getHttpPort()) {
            throw new IllegalArgumentException("selection metadata does not match held endpoint address");
        }
        if (serverStatus.getRole() == RoleType.DECODE
                && !(endpoint instanceof DecodeEndpoint)) {
            throw new IllegalArgumentException(
                    "Decode selection requires a Decode endpoint role");
        }
        if (placementVersion < 0L) {
            throw new IllegalArgumentException(
                    "placementVersion must be non-negative");
        }
        this.placementVersion = placementVersion;
    }

    public static SelectedRole decode(WorkerEndpoint.EndpointHold endpointHold, ServerStatus serverStatus) {
        return decode(endpointHold, serverStatus, endpointPlacementVersion(endpointHold));
    }

    public static SelectedRole decode(WorkerEndpoint.EndpointHold endpointHold,
                                      ServerStatus serverStatus,
                                      long placementVersion) {
        if (serverStatus == null || serverStatus.getRole() != RoleType.DECODE) {
            if (endpointHold != null) {
                endpointHold.close();
            }
            throw new IllegalArgumentException("Decode selection requires Decode metadata");
        }
        return createOwned(endpointHold, serverStatus, placementVersion);
    }

    public static SelectedRole stateless(WorkerEndpoint.EndpointHold endpointHold, ServerStatus serverStatus) {
        return createOwned(endpointHold, serverStatus, 0L);
    }

    /**
     * Calling a factory consumes the hold, including every validation failure.
     */
    private static SelectedRole createOwned(WorkerEndpoint.EndpointHold endpointHold,
                                            ServerStatus serverStatus,
                                            long placementVersion) {
        try {
            return new SelectedRole(endpointHold, serverStatus, placementVersion);
        } catch (RuntimeException | Error failure) {
            if (endpointHold != null) {
                endpointHold.close();
            }
            throw failure;
        }
    }

    public ServerStatus serverStatus() {
        return serverStatus;
    }

    public long placementVersion() {
        return placementVersion;
    }

    private static long endpointPlacementVersion(WorkerEndpoint.EndpointHold hold) {
        if (hold == null) {
            return 0L;
        }
        WorkerEndpoint endpoint = hold.endpoint();
        if (endpoint instanceof DecodeEndpoint decode) {
            return decode.placementVersion();
        }
        return 0L;
    }

    /**
     * Move the exact hold to the next domain owner.
     */
    public WorkerEndpoint.EndpointHold takeEndpointHold() {
        WorkerEndpoint.EndpointHold owned = requireOwnedHold();
        endpointHold = null;
        return owned;
    }

    private WorkerEndpoint.EndpointHold requireOwnedHold() {
        if (endpointHold == null) {
            throw new IllegalStateException(
                    "selected endpoint generation was already consumed");
        }
        return endpointHold;
    }

    @Override
    public void close() {
        WorkerEndpoint.EndpointHold owned = endpointHold;
        endpointHold = null;
        if (owned != null) {
            owned.close();
        }
    }
}
