package com.furkan.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DtoShipmentRequest {

    @NotNull(message = "Child order ID must not be null")
    private Long childOrderId;

    @NotBlank(message = "Warehouse information must not be blank")
    private String warehouse;

    @NotBlank(message = "Shipment type (mode) must not be blank")
    private String mode; // Örn: STANDARD, EXPRESS
}
