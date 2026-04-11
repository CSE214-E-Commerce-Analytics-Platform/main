package com.furkan.controllers.impl;

import com.furkan.controllers.IRestPaymentController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoPaymentRequest;
import com.furkan.dto.response.DtoPayment;
import com.furkan.enums.PaymentStatus;
import com.furkan.services.IPaymentService;
import com.furkan.utils.RootEntity;
import com.stripe.exception.StripeException;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class RestPaymentControllerImpl extends RestBaseController implements IRestPaymentController {

    private final IPaymentService paymentService;

    @PostMapping("/create")
    @Override
    public RootEntity<DtoPayment> createPayment(@RequestBody DtoPaymentRequest request) {
        return ok(paymentService.createPayment(request));
    }

    @GetMapping("/order/{orderId}")
    @Override
    public RootEntity<DtoPayment> findPaymentByOrderId(@PathVariable Long orderId) {
        return ok(paymentService.findPaymentByOrderId(orderId));
    }

    @GetMapping("/user/{userId}")
    @Override
    public RootEntity<List<DtoPayment>> findPaymentsByUserId(@PathVariable Long userId) {
        return ok(paymentService.findPaymentsByUserId(userId));
    }

    @GetMapping("/order/{orderId}/is-successful")
    @Override
    public RootEntity<Boolean> hasSuccessfulPayment(@PathVariable Long orderId) {
        return ok(paymentService.hasSuccessfulPayment(orderId));
    }

    @PutMapping("/order/{orderId}/status")
    @Override
    public RootEntity<DtoPayment> updatePaymentStatus(
            @PathVariable Long orderId,
            @RequestParam PaymentStatus newStatus,
            @RequestParam(required = false) String transactionKey) {
        return ok(paymentService.updatePaymentStatus(orderId, newStatus, transactionKey));
    }

    @PostMapping("/refund/order/{orderId}/item/{orderItemId}")
    @Override
    public RootEntity<DtoPayment> refundPaymentItem(
            @PathVariable Long orderId,
            @PathVariable Long orderItemId) throws StripeException {
        return ok(paymentService.refundPaymentItem(orderId, orderItemId));
    }

    @PostMapping("/webhook")
    @Override
    public RootEntity<Void> handleStripeWebhook(
            @RequestBody String payload,
            @RequestHeader("Stripe-Signature") String sigHeader) {
        paymentService.handleStripeWebhook(payload, sigHeader);
        return ok();
    }
}
