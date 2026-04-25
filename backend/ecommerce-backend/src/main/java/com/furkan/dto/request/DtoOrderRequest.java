package com.furkan.dto.request;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class DtoOrderRequest {
    @NotNull
    private Long addressId;
}
