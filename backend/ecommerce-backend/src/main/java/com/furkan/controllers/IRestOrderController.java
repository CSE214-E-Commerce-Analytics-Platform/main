package com.furkan.controllers;

import com.furkan.dto.request.DtoOrderRequest;
import com.furkan.dto.response.DtoOrder;
import com.furkan.enums.OrderStatus;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

public interface IRestOrderController {

    //  --- INDV ---
    RootEntity<DtoOrder> createOrder(UserDetails userDetails, DtoOrderRequest request);

    RootEntity<RestPageableEntity<DtoOrder>> findMyOrders(UserDetails userDetails, RestPageableRequest request);

    RootEntity<DtoOrder> findOrderById(Long orderId);

    RootEntity<Void> cancelOrder(Long orderId, UserDetails userDetails);

    // --- CORP ---
    RootEntity<RestPageableEntity<DtoOrder>> findOrdersByStoreId(Long storeId, UserDetails userDetails, RestPageableRequest request);

    RootEntity<DtoOrder> updateSubOrderStatus(Long subOrderId, OrderStatus status, Long storeId);

    // --- ADM ---
    RootEntity<RestPageableEntity<DtoOrder>> findAllOrders(RestPageableRequest request);
}
