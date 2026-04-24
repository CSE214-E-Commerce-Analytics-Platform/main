package com.furkan.services;

import com.furkan.dto.request.DtoOrderRequest;
import com.furkan.dto.response.DtoOrder;
import com.furkan.entities.Order;
import com.furkan.entities.OrderItem;
import com.furkan.enums.OrderStatus;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;

import java.util.List;

public interface IOrderService {

    //  --- INDV ---
    DtoOrder createOrder(Long userId, DtoOrderRequest request);

    RestPageableEntity<DtoOrder> findMyOrders(Long userId, RestPageableRequest request);

    DtoOrder findOrderById(Long orderId);

    void cancelOrder(Long orderId, Long userId);

    // --- CORP ---
    RestPageableEntity<DtoOrder> findOrdersByStoreId(Long storeId, Long userId, RestPageableRequest request);

    DtoOrder updateSubOrderStatus(Long subOrderId, OrderStatus status, Long storeId);

    // --- ADM ---
    RestPageableEntity<DtoOrder> findAllOrders(RestPageableRequest request);

    // Helper
    Order findEntityOrderById(Long orderId);

    void processItemRefund(Order order, OrderItem itemToRefund);
}
