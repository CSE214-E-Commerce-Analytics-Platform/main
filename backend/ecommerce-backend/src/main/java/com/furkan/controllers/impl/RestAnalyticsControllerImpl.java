package com.furkan.controllers.impl;

import com.furkan.controllers.IRestAnalyticsController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.response.DtoCustomerAnalytics;
import com.furkan.entities.User;
import com.furkan.services.IAnalyticsService;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/analytics")
@RequiredArgsConstructor
public class RestAnalyticsControllerImpl extends RestBaseController implements IRestAnalyticsController {

    private final IAnalyticsService analyticsService;

    @GetMapping("/store/{storeId}/customers")
    @PreAuthorize("hasAnyRole('CORPORATE', 'ADMIN')")
    @Override
    public RootEntity<DtoCustomerAnalytics> getStoreCustomerAnalytics(@PathVariable Long storeId, @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = ((User) userDetails).getId();
        return ok(analyticsService.getStoreCustomerAnalytics(storeId, userId));
    }
}
