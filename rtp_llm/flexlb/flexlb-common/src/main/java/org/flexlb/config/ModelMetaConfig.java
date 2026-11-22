package org.flexlb.config;

import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.flexlb.dao.route.Endpoint;
import org.flexlb.dao.route.RoleType;
import org.flexlb.dao.route.ServiceRoute;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
public class ModelMetaConfig {

    private static final List<RoleType> ROUTING_ORDER = List.of(
            RoleType.PDFUSION,
            RoleType.DECODE,
            RoleType.PREFILL,
            RoleType.VIT,
            RoleType.ENCODER);

    private final ServiceRoute serviceRoute;
    private final Set<RoleType> requiredRoles;

    public ModelMetaConfig(ServiceRoute serviceRoute) {
        this.serviceRoute = serviceRoute;
        this.requiredRoles = resolveRequiredRoles(serviceRoute);
    }

    private static Set<RoleType> resolveRequiredRoles(ServiceRoute serviceRoute) {
        List<RoleType> parsedRoles = serviceRoute.getAllRoleTypes();
        Set<RoleType> configured = parsedRoles.isEmpty()
                ? EnumSet.noneOf(RoleType.class)
                : EnumSet.copyOf(parsedRoles);
        return ROUTING_ORDER.stream()
                .filter(configured::contains)
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Return a fresh structural list for the configured role across groups.
     */
    public List<Pair<String, Endpoint>> endpointsWithGroup(RoleType role) {
        if (role == null || !serviceRoute.getAllRoleTypes().contains(role)) {
            return List.of();
        }
        return serviceRoute.getAllEndpointsWithGroup(role);
    }
}
