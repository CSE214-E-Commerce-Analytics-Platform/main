package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoProduct extends BaseDto {
    private String name;
    private String description;
    private String imageUrl;
    private String sku;
    private BigDecimal unitPrice;
    private Integer stockQuantity;
    private Long storeId;
    private String categoryName;
}
