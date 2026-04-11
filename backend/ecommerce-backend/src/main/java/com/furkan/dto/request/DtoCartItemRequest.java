package com.furkan.dto.request;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DtoCartItemRequest {

    @NotNull(message = "Product ID can't be null")
    private Long productId;

    @Min(value = 1, message = "The product quantity must be at least 1.")
    private int quantity;
}
