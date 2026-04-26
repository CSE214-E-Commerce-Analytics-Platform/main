package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import com.furkan.enums.OrderStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoOrder extends BaseDto {

    private OrderStatus status;
    private BigDecimal grandTotal;
    private LocalDateTime orderDate;
    private Long storeId;
    private String storeName;
    private Long parentOrderId;
    private List<DtoOrderItem> items;
    private List<DtoOrder> subOrders;
    private String fullAddress;
}
