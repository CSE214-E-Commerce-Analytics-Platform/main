package com.furkan.controllers.impl;

import com.furkan.controllers.IRestOrderController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoOrderRequest;
import com.furkan.dto.response.DtoOrder;
import com.furkan.entities.User;
import com.furkan.enums.OrderStatus;
import com.furkan.services.IOrderService;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class RestOrderControllerImpl extends RestBaseController implements IRestOrderController {

    private final IOrderService orderService;

    @PostMapping("/create")
    @PreAuthorize("hasRole('INDIVIDUAL')")
    @Override
    public RootEntity<DtoOrder> createOrder(@AuthenticationPrincipal UserDetails userDetails, @Valid @RequestBody DtoOrderRequest request) {
        return ok(orderService.createOrder(getUserIdByToken(userDetails), request));
    }

    @GetMapping("/my-orders")
    @PreAuthorize("hasRole('INDIVIDUAL')")
    @Override
    public RootEntity<RestPageableEntity<DtoOrder>> findMyOrders(@AuthenticationPrincipal UserDetails userDetails, @ModelAttribute RestPageableRequest request) {
        return ok(orderService.findMyOrders(getUserIdByToken(userDetails), request));
    }

    @GetMapping("/{orderId}")
    @PreAuthorize("hasAnyRole('ADMIN', 'INDIVIDUAL', 'CORPORATE')")
    @Override
    public RootEntity<DtoOrder> findOrderById(@PathVariable Long orderId) {
        return ok(orderService.findOrderById(orderId));
    }

    @PutMapping("/{orderId}/cancel")
    @PreAuthorize("hasRole('INDIVIDUAL')")
    @Override
    public RootEntity<Void> cancelOrder(@PathVariable Long orderId, @AuthenticationPrincipal UserDetails userDetails) {
        orderService.cancelOrder(orderId, getUserIdByToken(userDetails));
        return ok();
    }

    @GetMapping("/store/{storeId}")
    @PreAuthorize("hasRole('CORPORATE')")
    @Override
    public RootEntity<RestPageableEntity<DtoOrder>> findOrdersByStoreId(@PathVariable Long storeId, @AuthenticationPrincipal UserDetails userDetails, @ModelAttribute RestPageableRequest request) {
        return ok(orderService.findOrdersByStoreId(storeId, getUserIdByToken(userDetails), request));
    }

    @PatchMapping("/sub-order/{subOrderId}/status")
    @PreAuthorize("hasRole('CORPORATE')")
    @Override
    public RootEntity<DtoOrder> updateSubOrderStatus(@PathVariable Long subOrderId, @RequestParam OrderStatus status, @RequestParam Long storeId) {
        return ok(orderService.updateSubOrderStatus(subOrderId, status, storeId));
    }

    @GetMapping("/admin/all")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<RestPageableEntity<DtoOrder>> findAllOrders(@ModelAttribute RestPageableRequest request) {
        return ok(orderService.findAllOrders(request));
    }

    private Long getUserIdByToken(UserDetails userDetails) {
        return ((User) userDetails).getId();
    }
}
