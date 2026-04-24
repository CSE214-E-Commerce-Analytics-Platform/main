package com.furkan.controllers;

import com.furkan.dto.response.DtoCustomerAnalytics;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

public interface IRestAnalyticsController {
    RootEntity<DtoCustomerAnalytics> getStoreCustomerAnalytics(Long storeId, UserDetails userDetails);
}
