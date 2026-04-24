package com.furkan.services;

import com.furkan.dto.request.DtoPaymentRequest;
import com.furkan.dto.response.DtoPayment;
import com.furkan.enums.PaymentStatus;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.stripe.exception.StripeException;

public interface IPaymentService {

    DtoPayment createPayment(DtoPaymentRequest request);

    DtoPayment findPaymentByOrderId(Long orderId);

    RestPageableEntity<DtoPayment> findPaymentsByUserId(Long userId, RestPageableRequest request);

    boolean hasSuccessfulPayment(Long orderId);

    DtoPayment updatePaymentStatus(Long orderId, PaymentStatus newStatus, String transactionKey);

    DtoPayment refundPaymentItem(Long orderId, Long orderItemId) throws StripeException;

    void handleStripeWebhook(String payload, String sigHeader);
}
