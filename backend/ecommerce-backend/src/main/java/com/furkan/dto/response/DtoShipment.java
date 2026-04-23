package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import com.furkan.enums.ShipmentStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoShipment extends BaseDto {
    private Long orderId;
    private String trackingNumber;
    private String warehouse;
    private String mode;
    private ShipmentStatus status;
    private LocalDateTime estimatedDeliveryDate;
}
