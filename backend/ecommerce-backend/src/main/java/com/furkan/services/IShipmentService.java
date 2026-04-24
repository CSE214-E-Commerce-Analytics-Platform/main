package com.furkan.services;

import com.furkan.dto.request.DtoShipmentRequest;
import com.furkan.dto.response.DtoShipment;
import com.furkan.enums.ShipmentStatus;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;

public interface IShipmentService {

    DtoShipment initiateShipment(DtoShipmentRequest request, Long userId);

    DtoShipment updateStatusByAdmin(Long shipmentId, ShipmentStatus newStatus);

    DtoShipment findByTrackingNumber(String trackingNumber);

    DtoShipment cancelShipment(Long shipmentId);

    RestPageableEntity<DtoShipment> findAllShipments(RestPageableRequest request);

    RestPageableEntity<DtoShipment> findMyShipments(Long userId, RestPageableRequest request);

    DtoShipment findShipmentById(Long id, Long userId);

    DtoShipment findShipmentByOrderId(Long orderId, Long userId);
}
