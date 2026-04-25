package com.furkan.dto.response;

import lombok.Data;

import java.util.List;

@Data
public class DtoCustomerAnalytics {
    private int totalCustomers;
    private List<DtoCustomerSegment> segments;
    private List<DtoTopCustomer> topCustomers;
}
