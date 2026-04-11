package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoCart extends BaseDto {
    private Long userId;
    private List<DtoCartItem> items = new ArrayList<>();
    private BigDecimal totalPrice;
    private int totalItems;
}
