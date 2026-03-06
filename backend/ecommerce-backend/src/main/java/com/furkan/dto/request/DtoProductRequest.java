package com.furkan.dto.request;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class DtoProductRequest {
    @NotBlank(message = "Product name is required.")
    private String name;
    private String description;
    private String imageUrl;
    @NotBlank(message = "SKU must be unique.")
    private String sku;
    @Positive(message = "Price must be greater than 0.")
    private BigDecimal unitPrice;
    @Min(0)
    private Integer stockQuantity;
    private Long categoryId;
    private Long storeId;
}
