package com.furkan.services;

import com.furkan.dto.response.DtoCustomerAnalytics;

public interface IAnalyticsService {

    DtoCustomerAnalytics getStoreCustomerAnalytics(Long storeId, Long authenticatedUserId);
}
