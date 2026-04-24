package com.furkan.dto.response;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class DtoTopCustomer {
    private Long userId;
    private String email;
    private int totalOrders;
    private BigDecimal totalSpend;
}
