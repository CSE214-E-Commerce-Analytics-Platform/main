package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoCartItem extends BaseDto {

    private int quantity;
    private Long productId;
    private String productName;
    private BigDecimal productPrice;
    private String productImageUrl;
    private BigDecimal totalLinePrice;
}
