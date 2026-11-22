package org.flexlb.balance.scheduler;

import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.endpoint.EncoderEndpoint;
import org.flexlb.balance.strategy.EncoderStrategy;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.VitCacheSelector;
import org.flexlb.util.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class DefaultRouter {

    private final VitCacheSelector vitCacheSelector;
    private final EncoderStrategy encoderSelector;
    private final ConfigService configService;
    private final Set<RoleType> deployedRoles;

    @Autowired
    public DefaultRouter(EncoderStrategy encoderSelector,
                         ConfigService configService,
                         ModelMetaConfig modelMetaConfig,
                         VitCacheSelector vitCacheSelector) {
        this.encoderSelector = encoderSelector;
        this.configService = configService;
        this.deployedRoles = Collections.unmodifiableSet(new LinkedHashSet<>(modelMetaConfig.requiredRoles()));
        this.vitCacheSelector = vitCacheSelector;
    }

    boolean isEncoderOnly(BalanceContext context) {
        return requestedRoles(context).equals(Set.of(RoleType.ENCODER));
    }

    boolean hasEncoderRole() {
        return deployedRoles.contains(RoleType.ENCODER);
    }

    PlacementResult<EncoderEndpoint, PlacementKey> selectEncoder(BalanceContext context) {
        Response failure = validateRequest(context);
        if (failure != null) {
            return PlacementResult.rejected(failure);
        }
        if (!isEncoderOnly(context)) {
            return PlacementResult.rejected(Response.error(StrategyErrorType.INVALID_REQUEST));
        }
        String policyGroup = resolvePolicyGroup(context);
        EncoderEndpoint selected = encoderSelector.select(context, policyGroup);
        return selected == null
                ? PlacementResult.blocked(new PlacementKey(RoleType.ENCODER, policyGroup),
                        Response.error(StrategyErrorType.NO_ENCODER_WORKER))
                : PlacementResult.success(selected);
    }

    public Response routeVit(BalanceContext context) {
        Response invalid = validateRequest(context);
        if (invalid != null) { return invalid; }
        if (!deployedRoles.contains(RoleType.VIT) || vitCacheSelector == null) {
            return Response.error(StrategyErrorType.NO_VIT_WORKER);
        }
        ServerStatus vit = vitCacheSelector.select(context, resolvePolicyGroup(context));
        if (vit.isSuccess()) { return buildSuccessResponse(List.of(vit)); }
        Response failure = new Response();
        failure.setSuccess(false);
        failure.setCode(vit.getCode());
        failure.setErrorMessage(vit.getMessage());
        return failure;
    }

    public boolean selectedVitIsValid(BalanceContext context) {
        if (context.getRequest() == null || context.getRequest().getSelectedVit() == null) { return true; }
        return vitCacheSelector != null && deployedRoles.contains(RoleType.VIT)
                && vitCacheSelector.validate(context, resolvePolicyGroup(context)).isSuccess();
    }

    private Response validateRequest(BalanceContext context) {
        if (context == null || context.getRequest() == null) {
            Logger.error("masterRequest is null");
            return Response.error(StrategyErrorType.INVALID_REQUEST);
        }
        Set<RoleType> requested = context.getRequestedRoles();
        if (requested != null && !deployedRoles.containsAll(requested)) {
            return Response.error(StrategyErrorType.INVALID_REQUEST);
        }
        return null;
    }

    private Set<RoleType> requestedRoles(BalanceContext context) {
        Set<RoleType> requested = context.getRequestedRoles();
        return requested == null ? deployedRoles
                : deployedRoles.stream().filter(requested::contains)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public String resolvePolicyGroup(BalanceContext context) {
        if (context == null || context.getRequest() == null) {
            return null;
        }
        FlexlbConfig config = context.getConfig() != null
                ? context.getConfig()
                : configService.loadBalanceConfig();
        if (config == null || config.getRouter().getGroupSelector() == null) {
            return null;
        }
        return config.getRouter().getGroupSelector()
                .resolveTargetGroup(context.getRequest())
                .orElse(null);
    }

    private static Response buildSuccessResponse(List<ServerStatus> statuses) {
        Response response = new Response();
        response.setSuccess(true);
        response.setServerStatus(statuses);
        return response;
    }

}
