package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import com.furkan.enums.PaymentMethod;
import com.furkan.enums.PaymentStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.math.BigDecimal;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoPayment extends BaseDto {

    private Long orderId;
    private BigDecimal amount;
    private PaymentMethod method;
    private PaymentStatus status;
    private String transactionKey;
    private String errorMessage;
}
