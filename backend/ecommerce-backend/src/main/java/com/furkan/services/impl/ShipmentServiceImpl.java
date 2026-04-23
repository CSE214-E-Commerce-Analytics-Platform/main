package com.furkan.services.impl;

import com.furkan.dto.request.DtoShipmentRequest;
import com.furkan.dto.response.DtoShipment;
import com.furkan.entities.Order;
import com.furkan.entities.Shipment;
import com.furkan.entities.User;
import com.furkan.enums.OrderStatus;
import com.furkan.enums.RoleType;
import com.furkan.enums.ShipmentStatus;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.OrderRepository;
import com.furkan.repositories.ShipmentRepository;
import com.furkan.repositories.UserRepository;
import com.furkan.services.IShipmentService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Transactional
@RequiredArgsConstructor
public class ShipmentServiceImpl implements IShipmentService {

    private final ShipmentRepository shipmentRepository;
    private final OrderRepository orderRepository;
    private final UserRepository userRepository;

    @Override
    @Transactional
    public DtoShipment initiateShipment(DtoShipmentRequest request, Long userId) {
        Order childOrder = orderRepository.findById(request.getChildOrderId())
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.ORDER_NOT_FOUND, request.getChildOrderId().toString())));

        if (!childOrder.getStore().getOwner().getId().equals(userId)) {
            throw new BaseException(new ErrorMessage(MessageType.UNAUTHORIZED, "You can only ship orders belonging to your own store."));
        }

        if (childOrder.getParentOrder() == null) {
            throw new BaseException(new ErrorMessage(MessageType.SHIPMENT_FOR_NOT_CHILD_ORDER, request.getChildOrderId().toString()));
        }

        if (childOrder.getStatus() != OrderStatus.PAID) {
            throw new BaseException(new ErrorMessage(MessageType.ORDER_NOT_PAID, request.getChildOrderId().toString()));
        }

        String trackingNumber = "FRK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        Shipment shipment = new Shipment();
        shipment.setOrder(childOrder);
        shipment.setWarehouse(request.getWarehouse());
        shipment.setMode(request.getMode());
        shipment.setTrackingNumber(trackingNumber);
        shipment.setStatus(ShipmentStatus.IN_TRANSIT); // Mağaza verince direkt "Yolda" başlasın
        shipment.setCreatedAt(LocalDateTime.now());
        shipment.setUpdatedAt(LocalDateTime.now());
        shipment.setEstimatedDeliveryDate(LocalDateTime.now().plusDays(3));

        childOrder.setStatus(OrderStatus.SHIPPED);
        orderRepository.save(childOrder);

        updateMasterStatus(childOrder.getParentOrder());

        return dtoConverter(shipmentRepository.save(shipment));
    }

    @Override
    @Transactional
    public DtoShipment updateStatusByAdmin(Long shipmentId, ShipmentStatus newStatus) {
        Shipment shipment = shipmentRepository.findById(shipmentId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.SHIPMENT_TRACK_NOT_FOUND, shipmentId.toString())));

        shipment.setStatus(newStatus);
        shipment.setUpdatedAt(LocalDateTime.now());

        if (newStatus == ShipmentStatus.DELIVERED) {
            Order childOrder = shipment.getOrder();

            boolean allDelivered = shipmentRepository.findByOrderId(childOrder.getId()).stream()
                    .allMatch(s -> s.getStatus() == ShipmentStatus.DELIVERED);

            if (allDelivered) {
                childOrder.setStatus(OrderStatus.DELIVERED);
            } else {
                childOrder.setStatus(OrderStatus.PARTIALLY_DELIVERED);
            }

            orderRepository.save(childOrder);
            updateMasterStatus(childOrder.getParentOrder());
        }

        return dtoConverter(shipmentRepository.save(shipment));
    }

    @Override
    public DtoShipment findByTrackingNumber(String trackingNumber) {
        Shipment shipment = shipmentRepository.findByTrackingNumber(trackingNumber)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.SHIPMENT_NOT_FOUND_FOR_TR_NUM, trackingNumber)));
        return dtoConverter(shipment);
    }

    @Override
    @Transactional
    public DtoShipment cancelShipment(Long shipmentId) {
        Shipment shipment = shipmentRepository.findById(shipmentId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.SHIPMENT_NOT_FOUND, shipmentId.toString())));

        if (shipment.getStatus() == ShipmentStatus.IN_TRANSIT ||
                shipment.getStatus() == ShipmentStatus.DELIVERED ||
                shipment.getStatus() == ShipmentStatus.OUT_FOR_DELIVERY) {
            throw new BaseException(new ErrorMessage(MessageType.SHIPMENT_CAN_NOT_CANCEL, shipmentId.toString()));
        }

        shipment.setStatus(ShipmentStatus.CANCELLED);
        shipment.setUpdatedAt(LocalDateTime.now());

        Shipment canceledShipment = shipmentRepository.save(shipment);

        Order childOrder = shipment.getOrder();

        boolean hasOtherActiveShipments = shipmentRepository.findByOrderId(childOrder.getId()).stream()
                .anyMatch(s -> s.getStatus() != ShipmentStatus.CANCELLED);

        if (!hasOtherActiveShipments) {
            childOrder.setStatus(OrderStatus.PAID);
            orderRepository.save(childOrder);
            updateMasterStatus(childOrder.getParentOrder());
        }

        return dtoConverter(canceledShipment);
    }

    @Override
    public List<DtoShipment> findAllShipments() {
        return shipmentRepository.findAll()
                .stream()
                .map(this::dtoConverter)
                .collect(Collectors.toList());
    }

    @Override
    public List<DtoShipment> findMyShipments(Long userId) {
        return shipmentRepository.findByUserId(userId)
                .stream()
                .map(this::dtoConverter)
                .collect(Collectors.toList());
    }

    @Override
    public DtoShipment findShipmentById(Long id, Long userId) {
        Shipment shipment = shipmentRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.SHIPMENT_NOT_FOUND, id.toString())));

        verifyShipmentAccess(shipment.getOrder(), userId);

        return dtoConverter(shipment);
    }

    @Override
    public DtoShipment findShipmentByOrderId(Long orderId, Long userId) {
        Shipment shipment = shipmentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.SHIPMENT_NOT_FOUND_FOR_ORDER_ID, orderId.toString())));

        verifyShipmentAccess(shipment.getOrder(), userId);

        return dtoConverter(shipment);
    }

    private void verifyShipmentAccess(Order order, Long userId) {
        User currentUser = userRepository.findById(userId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, userId.toString())));

        boolean isAdmin = currentUser.getRoleType() == RoleType.ADMIN;
        boolean isBuyer = order.getUser().getId().equals(userId);
        boolean isSeller = order.getStore().getOwner().getId().equals(userId);

        if (!isAdmin && !isBuyer && !isSeller) {
            throw new BaseException(new ErrorMessage(MessageType.UNAUTHORIZED, "You do not have permission to view this shipping information."));
        }
    }

    private DtoShipment dtoConverter(Shipment shipment) {
        DtoShipment dto = new DtoShipment();
        org.springframework.beans.BeanUtils.copyProperties(shipment, dto);
        dto.setOrderId(shipment.getOrder().getId());
        return dto;
    }

    private void updateMasterStatus(Order masterOrder) {
        if (masterOrder == null) return;

        List<Order> subOrders = masterOrder.getSubOrders();

        boolean allDelivered = subOrders.stream().allMatch(o -> o.getStatus() == OrderStatus.DELIVERED);
        boolean anyDelivered = subOrders.stream().anyMatch(o -> o.getStatus() == OrderStatus.DELIVERED || o.getStatus() == OrderStatus.PARTIALLY_DELIVERED);

        boolean allShipped = subOrders.stream().allMatch(o -> o.getStatus() == OrderStatus.SHIPPED || o.getStatus() == OrderStatus.DELIVERED);
        boolean anyShipped = subOrders.stream().anyMatch(o -> o.getStatus() == OrderStatus.SHIPPED || o.getStatus() == OrderStatus.PARTIALLY_SHIPPED);

        if (allDelivered) {
            masterOrder.setStatus(OrderStatus.DELIVERED);
        } else if (anyDelivered) {
            masterOrder.setStatus(OrderStatus.PARTIALLY_DELIVERED);
        } else if (allShipped) {
            masterOrder.setStatus(OrderStatus.SHIPPED);
        } else if (anyShipped) {
            masterOrder.setStatus(OrderStatus.PARTIALLY_SHIPPED);
        }

        orderRepository.save(masterOrder);
    }
}
