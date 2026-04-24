package com.furkan.controllers;

import com.furkan.dto.request.DtoOrderRequest;
import com.furkan.dto.response.DtoOrder;
import com.furkan.enums.OrderStatus;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

public interface IRestOrderController {

    //  --- INDV ---
    RootEntity<DtoOrder> createOrder(UserDetails userDetails, DtoOrderRequest request);

    RootEntity<List<DtoOrder>> findMyOrders(UserDetails userDetails);

    RootEntity<DtoOrder> findOrderById(Long orderId);

    RootEntity<Void> cancelOrder(Long orderId, UserDetails userDetails);

    // --- CORP ---
    RootEntity<List<DtoOrder>> findOrdersByStoreId(Long storeId, UserDetails userDetails);

    RootEntity<DtoOrder> updateSubOrderStatus(Long subOrderId, OrderStatus status, Long storeId);

    // --- ADM ---
    RootEntity<List<DtoOrder>> findAllOrders();
}
