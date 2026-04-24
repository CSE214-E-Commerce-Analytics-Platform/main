package com.furkan.controllers;

import com.furkan.dto.request.DtoShipmentRequest;
import com.furkan.dto.response.DtoShipment;
import com.furkan.enums.ShipmentStatus;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

public interface IRestShipmentController {

    RootEntity<DtoShipment> initializeShipment(DtoShipmentRequest request, UserDetails userDetails);

    RootEntity<DtoShipment> updateShipmentStatus(Long shipmentId, ShipmentStatus newStatus);

    RootEntity<DtoShipment> trackShipment(String trackingNumber);

    RootEntity<DtoShipment> cancelShipment(Long shipmentId);

    RootEntity<RestPageableEntity<DtoShipment>> findAllShipments(RestPageableRequest request);

    RootEntity<RestPageableEntity<DtoShipment>> findMyShipments(UserDetails userDetails, RestPageableRequest request);

    RootEntity<DtoShipment> findShipmentById(Long id, UserDetails userDetails);

    RootEntity<DtoShipment> findShipmentByOrderId(Long orderId, UserDetails userDetails);
}
