package org.flexlb.dao.route;

import org.flexlb.balance.prediction.DecodeCostFormula;
import org.flexlb.enums.DecodeMode;

/**
 * Conditions a Decode worker must satisfy for one request, frozen before selection.
 * inputKvTokens is max(0, seqLen). inputAndMaxOutputKvTokens is
 * inputKvTokens + max(0, maxNewTokens), saturated at Long.MAX_VALUE.
 * Neither value is an estimate of the actual generated output length.
 */
public record DecodeRequirement(String requestId,
                                int priority,
                                long inputKvTokens,
                                long inputAndMaxOutputKvTokens,
                                CapacityLimit capacity,
                                DecodeMode mode,
                                DecodeCostFormula costFormula) {
}
