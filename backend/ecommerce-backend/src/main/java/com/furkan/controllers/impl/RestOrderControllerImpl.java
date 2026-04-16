package com.furkan.controllers.impl;

import com.furkan.controllers.IRestOrderController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.response.DtoOrder;
import com.furkan.entities.User;
import com.furkan.enums.OrderStatus;
import com.furkan.services.IOrderService;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class RestOrderControllerImpl extends RestBaseController implements IRestOrderController {

    private final IOrderService orderService;

    @PostMapping("/create")
    @PreAuthorize("hasRole('INDIVIDUAL')")
    @Override
    public RootEntity<DtoOrder> createOrder(@AuthenticationPrincipal UserDetails userDetails) {
        return ok(orderService.createOrder(getUserIdByToken(userDetails)));
    }

    @GetMapping("/my-orders")
    @PreAuthorize("hasRole('INDIVIDUAL')")
    @Override
    public RootEntity<List<DtoOrder>> findMyOrders(@AuthenticationPrincipal UserDetails userDetails) {
        return ok(orderService.findMyOrders(getUserIdByToken(userDetails)));
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
    public RootEntity<List<DtoOrder>> findOrdersByStoreId(@PathVariable Long storeId, @AuthenticationPrincipal UserDetails userDetails) {
        return ok(orderService.findOrdersByStoreId(storeId, getUserIdByToken(userDetails)));
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
    public RootEntity<List<DtoOrder>> findAllOrders() {
        return ok(orderService.findAllOrders());
    }

    private Long getUserIdByToken(UserDetails userDetails) {
        return ((User) userDetails).getId();
    }
}
