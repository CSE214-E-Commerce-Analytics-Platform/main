package com.furkan.controllers;

import com.furkan.dto.request.DtoShipmentRequest;
import com.furkan.dto.response.DtoShipment;
import com.furkan.enums.ShipmentStatus;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

public interface IRestShipmentController {

    RootEntity<DtoShipment> initializeShipment(DtoShipmentRequest request, UserDetails userDetails);

    RootEntity<DtoShipment> updateShipmentStatus(Long shipmentId, ShipmentStatus newStatus);

    RootEntity<DtoShipment> trackShipment(String trackingNumber);

    RootEntity<DtoShipment> cancelShipment(Long shipmentId);

    RootEntity<List<DtoShipment>> findAllShipments();

    RootEntity<List<DtoShipment>> findMyShipments(UserDetails userDetails);

    RootEntity<DtoShipment> findShipmentById(Long id, UserDetails userDetails);

    RootEntity<DtoShipment> findShipmentByOrderId(Long orderId, UserDetails userDetails);
}
