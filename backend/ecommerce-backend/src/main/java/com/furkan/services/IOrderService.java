package com.furkan.services;

import com.furkan.dto.request.DtoOrderRequest;
import com.furkan.dto.response.DtoOrder;
import com.furkan.entities.Order;
import com.furkan.entities.OrderItem;
import com.furkan.enums.OrderStatus;

import java.util.List;

public interface IOrderService {

    //  --- INDV ---
    DtoOrder createOrder(Long userId, DtoOrderRequest request);

    List<DtoOrder> findMyOrders(Long userId);

    DtoOrder findOrderById(Long orderId);

    void cancelOrder(Long orderId, Long userId);

    // --- CORP ---
    List<DtoOrder> findOrdersByStoreId(Long storeId, Long userId);

    DtoOrder updateSubOrderStatus(Long subOrderId, OrderStatus status, Long storeId);

    // --- ADM ---
    List<DtoOrder> findAllOrders();

    // Helper
    Order findEntityOrderById(Long orderId);

    void processItemRefund(Order order, OrderItem itemToRefund);
}
