package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.DecodeEndpoint;

public record SelectedDecode(DecodeEndpoint endpoint, boolean selectedAsEmpty) {}
