package com.furkan.services;

import com.furkan.dto.request.DtoPaymentRequest;
import com.furkan.dto.response.DtoPayment;
import com.furkan.enums.PaymentStatus;
import com.stripe.exception.StripeException;

import java.util.List;

public interface IPaymentService {

    DtoPayment createPayment(DtoPaymentRequest request);

    DtoPayment findPaymentByOrderId(Long orderId);

    List<DtoPayment> findPaymentsByUserId(Long userId);

    boolean hasSuccessfulPayment(Long orderId);

    DtoPayment updatePaymentStatus(Long orderId, PaymentStatus newStatus, String transactionKey);

    DtoPayment refundPaymentItem(Long orderId, Long orderItemId) throws StripeException;

    void handleStripeWebhook(String payload, String sigHeader);
}
