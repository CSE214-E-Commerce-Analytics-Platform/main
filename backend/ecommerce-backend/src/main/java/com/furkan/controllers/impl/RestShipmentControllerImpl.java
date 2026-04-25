package com.furkan.controllers.impl;

import com.furkan.controllers.IRestShipmentController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoShipmentRequest;
import com.furkan.dto.response.DtoShipment;
import com.furkan.entities.User;
import com.furkan.enums.ShipmentStatus;
import com.furkan.services.IShipmentService;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/shipments")
@RequiredArgsConstructor
public class RestShipmentControllerImpl extends RestBaseController implements IRestShipmentController {

    private final IShipmentService shipmentService;

    @PostMapping("/initialize")
    @PreAuthorize("hasRole('CORPORATE')")
    @Override
    public RootEntity<DtoShipment> initializeShipment(@Valid @RequestBody DtoShipmentRequest request, @AuthenticationPrincipal UserDetails userDetails) {
        return ok(shipmentService.initiateShipment(request, getUserIdByToken(userDetails)));
    }

    @PutMapping("/{shipmentId}/status")
    @Override
    @PreAuthorize("hasRole('ADMIN')")
    public RootEntity<DtoShipment> updateShipmentStatus(@PathVariable Long shipmentId, @RequestParam ShipmentStatus newStatus) {
        return ok(shipmentService.updateStatusByAdmin(shipmentId, newStatus));
    }

    @GetMapping("/track/{trackingNumber}")
    @Override
    public RootEntity<DtoShipment> trackShipment(@PathVariable String trackingNumber) {
        return ok(shipmentService.findByTrackingNumber(trackingNumber));
    }

    @PutMapping("/{shipmentId}/cancel")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<DtoShipment> cancelShipment(@PathVariable Long shipmentId) {
        return ok(shipmentService.cancelShipment(shipmentId));
    }

    @GetMapping()
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<RestPageableEntity<DtoShipment>> findAllShipments(@ModelAttribute RestPageableRequest request) {
        return ok(shipmentService.findAllShipments(request));
    }

    @GetMapping("/my-shipments")
    @Override
    public RootEntity<RestPageableEntity<DtoShipment>> findMyShipments(@AuthenticationPrincipal UserDetails userDetails, @ModelAttribute RestPageableRequest request) {
        return ok(shipmentService.findMyShipments(getUserIdByToken(userDetails), request));
    }

    @GetMapping("/{id}")
    @Override
    public RootEntity<DtoShipment> findShipmentById(@PathVariable Long id, @AuthenticationPrincipal UserDetails userDetails) {
        return ok(shipmentService.findShipmentById(id, getUserIdByToken(userDetails)));
    }

    @GetMapping("/order/{orderId}")
    @Override
    public RootEntity<DtoShipment> findShipmentByOrderId(@PathVariable Long orderId, @AuthenticationPrincipal UserDetails userDetails) {
        return ok(shipmentService.findShipmentByOrderId(orderId, getUserIdByToken(userDetails)));
    }

    private Long getUserIdByToken(UserDetails userDetails) {
        return ((User) userDetails).getId();
    }
}
