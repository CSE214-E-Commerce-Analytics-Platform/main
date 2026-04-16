package com.furkan.dto.request;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class DtoOrderRequest {
    private String shippingAddress;
    private BigDecimal shippingCost;
}
