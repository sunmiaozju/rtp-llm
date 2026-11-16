package org.flexlb.dao.loadbalance;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class Response {

    @JsonProperty("server_status")
    private List<ServerStatus> serverStatus;

    @JsonProperty("success")
    private boolean success;

    @JsonProperty("code")
    private int code = 200;

    @JsonProperty("error_message")
    private String errorMessage;

    @JsonProperty("real_master_host")
    private String realMasterHost;

    @JsonProperty("queue_length")
    private Integer queueLength;

    @JsonProperty("enqueued_by_master")
    private boolean enqueuedByMaster = false;

    @JsonProperty("worker_summary")
    private Map<String, WorkerRoleSummary> workerSummary;

    @JsonProperty("ready")
    private boolean ready = true;

    @JsonProperty("admission_reject_reason")
    private RejectReason admissionRejectReason = RejectReason.UNSPECIFIED;

    /**
     * Builds a successful delivery response without changing the original response.
     * Selected worker data is read-only after routing and is shared with the delivery response.
     */
    public static Response buildSuccessResponse(Response routeResponse, boolean enqueuedByMaster) {
        Response response = new Response();
        response.serverStatus = routeResponse.serverStatus;
        response.success = true;
        response.code = 200;
        response.errorMessage = routeResponse.errorMessage;
        response.realMasterHost = routeResponse.realMasterHost;
        response.queueLength = routeResponse.queueLength;
        response.enqueuedByMaster = enqueuedByMaster;
        response.workerSummary = routeResponse.workerSummary;
        response.ready = routeResponse.ready;
        response.admissionRejectReason = routeResponse.admissionRejectReason;
        return response;
    }

    public static Response buildErrorResponse(StrategyErrorType errorType, String message) {
        return error(errorType,
                errorType == StrategyErrorType.RESOURCE_EXHAUSTED
                        ? RejectReason.RESOURCE_EXHAUSTED
                        : RejectReason.UNSPECIFIED,
                message);
    }

    public static Response error(StrategyErrorType strategyErrorType) {
        return error(strategyErrorType,
                strategyErrorType == StrategyErrorType.RESOURCE_EXHAUSTED
                        ? RejectReason.RESOURCE_EXHAUSTED
                        : RejectReason.UNSPECIFIED);
    }

    public static Response error(StrategyErrorType strategyErrorType, RejectReason admissionRejectReason) {
        return error(strategyErrorType, admissionRejectReason, null);
    }

    public static Response error(StrategyErrorType strategyErrorType,
                                 RejectReason admissionRejectReason,
                                 String detail) {
        if (admissionRejectReason == null) {
            admissionRejectReason = RejectReason.UNSPECIFIED;
        }
        if (!strategyErrorType.acceptsRejectReason(admissionRejectReason)) {
            throw new IllegalArgumentException("invalid schedule error code/reason: "
                    + strategyErrorType + "/" + admissionRejectReason);
        }
        Response result = new Response();
        result.setSuccess(false);
        result.setCode(strategyErrorType.getErrorCode());
        detail = switch (strategyErrorType) {
            case PRIORITY_ADMISSION_REJECTED -> admissionRejectReason == RejectReason.HIGHER_PRIORITY_AHEAD
                    ? "higher-priority requests are ahead" : "same-priority requests are ahead";
            case ADMISSION_UNAVAILABLE -> "admission unavailable; blocker priority attribution is unavailable";
            case RESOURCE_EXHAUSTED -> {
                String standard = "admission capacity is temporarily exhausted";
                yield detail == null || detail.isBlank() ? standard
                        : detail.startsWith(standard) ? detail : standard + "; trigger=" + detail;
            }
            default -> detail;
        };
        result.setErrorMessage(strategyErrorType.buildErrorMessage(detail));
        result.setAdmissionRejectReason(admissionRejectReason);
        return result;
    }

    @Data
    public static class WorkerRoleSummary {
        private int discovered;
        private int alive;
        private long maxQueueTokens;
    }
}
