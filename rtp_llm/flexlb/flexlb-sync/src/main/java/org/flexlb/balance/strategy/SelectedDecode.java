package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.DecodeEndpoint;

public record SelectedDecode(DecodeEndpoint endpoint, boolean selectedAsEmpty, boolean allCandidatesFull) {

    public SelectedDecode(DecodeEndpoint endpoint, boolean selectedAsEmpty) {
        this(endpoint, selectedAsEmpty, false);
    }

    public static SelectedDecode whenAllCandidatesFull(DecodeEndpoint endpoint) {
        return new SelectedDecode(endpoint, false, true);
    }
}
