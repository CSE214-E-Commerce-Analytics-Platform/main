package com.furkan.controllers;

import com.furkan.dto.request.DtoPaymentRequest;
import com.furkan.dto.response.DtoPayment;
import com.furkan.enums.PaymentStatus;
import com.furkan.utils.RootEntity;
import com.stripe.exception.StripeException;

import java.util.List;

public interface IRestPaymentController {

    RootEntity<DtoPayment> createPayment(DtoPaymentRequest request);

    RootEntity<DtoPayment> findPaymentByOrderId(Long orderId);

    RootEntity<List<DtoPayment>> findPaymentsByUserId(Long userId);

    RootEntity<Boolean> hasSuccessfulPayment(Long orderId);

    RootEntity<DtoPayment> updatePaymentStatus(Long orderId, PaymentStatus newStatus, String transactionKey);

    RootEntity<DtoPayment> refundPaymentItem(Long orderId, Long orderItemId) throws StripeException;

    RootEntity<Void> handleStripeWebhook(String payload, String sigHeader);
}
