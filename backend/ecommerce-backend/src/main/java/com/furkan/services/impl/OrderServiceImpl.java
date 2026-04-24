package com.furkan.services.impl;

import com.furkan.dto.request.DtoOrderRequest;
import com.furkan.dto.response.DtoOrder;
import com.furkan.dto.response.DtoOrderItem;
import com.furkan.entities.*;
import com.furkan.enums.OrderStatus;
import com.furkan.enums.PaymentStatus;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.*;
import com.furkan.services.ICartService;
import com.furkan.services.IOrderService;
import com.furkan.services.IProductService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements IOrderService {

    private final OrderRepository orderRepository;
    private final ICartService cartService;
    private final IProductService productService;
    private final UserRepository userRepository;
    private final StoreRepository storeRepository;
    private final PaymentRepository paymentRepository;
    private final AddressRepository addressRepository;

    @Override
    @Transactional
    public DtoOrder createOrder(Long userId, DtoOrderRequest request) {
        Cart cart = cartService.findEntityCartByUserId(userId);
        if (cart.getCartItems().isEmpty()) {
            throw new BaseException(new ErrorMessage(MessageType.CART_IS_EMPTY, userId.toString()));
        }

        Address address = addressRepository.findById(request.getAddressId())
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.ADDRESS_NOT_FOUND, request.getAddressId().toString())));

        if (!address.getUser().getId().equals(userId)) {
            throw new BaseException(new ErrorMessage(MessageType.ADDRESS_USER_MISMATCH, address.getId().toString()));
        }

        Order masterOrder = new Order();
        masterOrder.setUser(cart.getUser());
        masterOrder.setOrderDate(LocalDateTime.now());
        masterOrder.setStatus(OrderStatus.PENDING);
        masterOrder.setGrandTotal(cart.getTotalPrice());
        masterOrder.setSubOrders(new ArrayList<>());
        masterOrder.setAddress(address);

        Map<Long, List<CartItem>> itemsByStore = cart.getCartItems().stream()
                .collect(Collectors.groupingBy(item -> item.getProduct().getStore().getId()));

        for (Map.Entry<Long, List<CartItem>> entry : itemsByStore.entrySet()) {
            Long storeId = entry.getKey();
            List<CartItem> storeItems = entry.getValue();

            Order subOrder = createSubOrder(masterOrder, storeId, storeItems);
            masterOrder.getSubOrders().add(subOrder);
        }

        Order savedMaster = orderRepository.save(masterOrder);

        updateStocks(cart.getCartItems());
        cartService.clearCart(userId);

        return dtoOrderMapper(savedMaster);
    }

    @Override
    public List<DtoOrder> findMyOrders(Long userId) {
        List<Order> masterOrder = orderRepository.findByUserIdAndParentOrderIsNullOrderByOrderDateDesc(userId);

        return masterOrder.stream()
                .map(this::dtoOrderMapper)
                .collect(Collectors.toList());
    }

    @Override
    public DtoOrder findOrderById(Long orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.ORDER_NOT_FOUND, orderId.toString())));

        return dtoOrderMapper(order);
    }

    @Override
    @Transactional
    public void cancelOrder(Long orderId, Long userId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.ORDER_NOT_FOUND, orderId.toString())));

        if (!order.getUser().getId().equals(userId)) {
            throw new BaseException(new ErrorMessage(MessageType.UNAUTHORIZED, "You can not cancel this order."));
        }

        if (!order.getStatus().equals(OrderStatus.PENDING)) {
            throw new BaseException(new ErrorMessage(MessageType.ORDER_CANNOT_BE_CANCELLED, order.getStatus().name()));
        }

        order.setStatus(OrderStatus.CANCELLED);

        paymentRepository.findByOrderId(orderId).ifPresent(payment -> {
            payment.setStatus(PaymentStatus.FAILED);
            paymentRepository.save(payment);
        });

        if (order.getSubOrders() != null && !order.getSubOrders().isEmpty()) {
            for (Order subOrder : order.getSubOrders()) {
                subOrder.setStatus(OrderStatus.CANCELLED);

                restockItems(subOrder.getOrderItems());
            }
        } else {
            restockItems(order.getOrderItems());
        }

        orderRepository.save(order);
    }

    @Override
    public List<DtoOrder> findOrdersByStoreId(Long storeId, Long userId) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, userId.toString())));

        if (!user.getStore().getId().equals(storeId)) {
            throw new BaseException(new ErrorMessage(MessageType.UNAUTHORIZED, null));
        }

        List<Order> subOrders = orderRepository.findByStoreIdOrderByOrderDateDesc(storeId);

        return subOrders.stream()
                .map(this::dtoOrderMapper)
                .collect(Collectors.toList());
    }

    @Override
    @Transactional
    public DtoOrder updateSubOrderStatus(Long subOrderId, OrderStatus status, Long storeId) {
        Order subOrder = orderRepository.findById(subOrderId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.ORDER_NOT_FOUND, subOrderId.toString())));

        if (!subOrder.getStore().getId().equals(storeId)) {
            throw new BaseException(new ErrorMessage(MessageType.UNAUTHORIZED, "You cannot update an order that is not from this store."));
        }

        subOrder.setStatus(status);
        Order savedSubOrder = orderRepository.save(subOrder);

        if (subOrder.getParentOrder() != null) {
            updateMasterOrderStatus(subOrder.getParentOrder());
        }

        return dtoOrderMapper(savedSubOrder);
    }

    @Override
    public List<DtoOrder> findAllOrders() {
        return orderRepository.findAll().stream()
                .map(this::dtoOrderMapper)
                .collect(Collectors.toList());
    }

    @Override
    public Order findEntityOrderById(Long orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.ORDER_NOT_FOUND, orderId.toString())));
    }

    @Override
    public void processItemRefund(Order order, OrderItem itemToRefund) {
        productService.increaseStock(itemToRefund.getProduct().getId(), itemToRefund.getQuantity());
        orderRepository.save(order);
    }

    private Order createSubOrder(Order masterOrder, Long storeId, List<CartItem> cartItems) {
        Store store = storeRepository.findById(storeId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.STORE_NOT_FOUND, storeId.toString())));

        Order subOrder = new Order();
        subOrder.setParentOrder(masterOrder);
        subOrder.setUser(masterOrder.getUser());
        subOrder.setStore(store);
        subOrder.setOrderDate(masterOrder.getOrderDate());
        subOrder.setStatus(OrderStatus.PENDING);
        subOrder.setOrderItems(new ArrayList<>());
        subOrder.setAddress(masterOrder.getAddress());

        BigDecimal subTotal = BigDecimal.ZERO;

        for (CartItem ci : cartItems) {
            OrderItem oi = new OrderItem();
            oi.setOrder(subOrder);
            oi.setProduct(ci.getProduct());
            oi.setPrice(ci.getProduct().getUnitPrice());
            oi.setQuantity(ci.getQuantity());

            subOrder.getOrderItems().add(oi);
            subTotal = subTotal.add(oi.getPrice().multiply(new BigDecimal(oi.getQuantity())));
        }

        subOrder.setGrandTotal(subTotal);
        return subOrder;
    }

    private void updateStocks(List<CartItem> items) {
        for (CartItem item : items) {
            productService.reduceStock(item.getProduct().getId(), item.getQuantity());
        }
    }

    private void restockItems(List<OrderItem> orderItems) {
        for (OrderItem item : orderItems) {
            productService.increaseStock(item.getProduct().getId(), item.getQuantity());
        }
    }

    private void updateMasterOrderStatus(Order masterOrder) {
        List<OrderStatus> subStatuses = masterOrder.getSubOrders().stream()
                .map(Order::getStatus)
                .collect(Collectors.toList());

        boolean allSame = subStatuses.stream().allMatch(s -> s == subStatuses.get(0));

        if (allSame) {
            masterOrder.setStatus(subStatuses.get(0));
        } else if (subStatuses.contains(OrderStatus.SHIPPED) || subStatuses.contains(OrderStatus.DELIVERED)) {
            masterOrder.setStatus(OrderStatus.PARTIALLY_SHIPPED);
        } else {
            masterOrder.setStatus(OrderStatus.PENDING);
        }

        orderRepository.save(masterOrder);
    }

    private DtoOrder dtoOrderMapper(Order order) {
        if (order == null) return null;

        DtoOrder dtoOrder = new DtoOrder();
        BeanUtils.copyProperties(order, dtoOrder);

        dtoOrder.setItems(new ArrayList<>());
        dtoOrder.setSubOrders(new ArrayList<>());

        if (order.getOrderItems() != null && !order.getOrderItems().isEmpty()) {
            for (OrderItem oi : order.getOrderItems()) {
                DtoOrderItem itemDto = new DtoOrderItem();
                itemDto.setId(oi.getId());
                itemDto.setQuantity(oi.getQuantity());
                itemDto.setPrice(oi.getPrice());
                itemDto.setProductId(oi.getProduct().getId());
                itemDto.setProductName(oi.getProduct().getName());

                dtoOrder.getItems().add(itemDto);
            }
        }

        if (order.getSubOrders() != null && !order.getSubOrders().isEmpty()) {
            for (Order sub : order.getSubOrders()) {
                dtoOrder.getSubOrders().add(dtoOrderMapper(sub));
            }
        }

        if (order.getStore() != null) {
            dtoOrder.setStoreId(order.getStore().getId());
            dtoOrder.setStoreName(order.getStore().getName());
        }

        if (order.getAddress() != null) {
            dtoOrder.setFullAddress(order.getAddress().getFullAddress());
        }

        return dtoOrder;
    }
}
