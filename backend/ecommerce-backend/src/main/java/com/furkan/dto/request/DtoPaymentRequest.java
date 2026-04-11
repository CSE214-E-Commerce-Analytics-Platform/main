package com.furkan.dto.request;

import com.furkan.enums.PaymentMethod;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DtoPaymentRequest {

    @NotNull
    private Long orderId;

    private String stripeToken;

    private PaymentMethod paymentMethod = PaymentMethod.CREDIT_CARD; // Default
}
