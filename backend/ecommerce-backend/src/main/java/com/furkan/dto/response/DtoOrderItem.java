package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoOrderItem extends BaseDto {
    private Long productId;
    private String productName;
    private int quantity;
    private BigDecimal price;
}
