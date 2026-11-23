package org.flexlb.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.flexlb.constant.CommonConstants;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CommonUtils {

    /**
     * Convert HTTP port to gRPC port
     *
     * @param httpPort http port
     * @return gRPC port
     */
    public static int toGrpcPort(int httpPort) {
        return httpPort + CommonConstants.GRPC_PORT_OFFSET;
    }

    public static long saturatedAddNonNegative(long left, long right) {
        if (left < 0 || right < 0) {
            throw new IllegalArgumentException("KV admission counters must be non-negative");
        }
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
