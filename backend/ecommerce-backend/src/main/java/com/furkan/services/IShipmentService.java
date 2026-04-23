package com.furkan.services;

import com.furkan.dto.request.DtoShipmentRequest;
import com.furkan.dto.response.DtoShipment;
import com.furkan.enums.ShipmentStatus;

import java.util.List;

public interface IShipmentService {

    DtoShipment initiateShipment(DtoShipmentRequest request, Long userId);

    DtoShipment updateStatusByAdmin(Long shipmentId, ShipmentStatus newStatus);

    DtoShipment findByTrackingNumber(String trackingNumber);

    DtoShipment cancelShipment(Long shipmentId);

    List<DtoShipment> findAllShipments();

    List<DtoShipment> findMyShipments(Long userId);

    DtoShipment findShipmentById(Long id, Long userId);

    DtoShipment findShipmentByOrderId(Long orderId, Long userId);
}
