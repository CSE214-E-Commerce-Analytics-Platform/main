package com.furkan.services.impl;

import com.furkan.dto.request.DtoPaymentRequest;
import com.furkan.dto.response.DtoPayment;
import com.furkan.enums.OrderStatus;
import com.furkan.enums.PaymentMethod;
import com.furkan.enums.PaymentStatus;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.entities.Order;
import com.furkan.entities.OrderItem;
import com.furkan.entities.Payment;
import com.furkan.repositories.OrderRepository;
import com.furkan.repositories.PaymentRepository;
import com.furkan.services.IEmailService;
import com.furkan.services.IOrderService;
import com.furkan.services.IPaymentService;
import com.stripe.Stripe;
import com.stripe.exception.StripeException;
import com.stripe.model.*;
import com.stripe.model.checkout.Session;
import com.stripe.param.RefundCreateParams;
import com.stripe.param.checkout.SessionCreateParams;
import com.stripe.net.Webhook;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

// --- Java Standart Kütüphane Importları ---
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Transactional
@RequiredArgsConstructor
public class PaymentServiceImpl implements IPaymentService {

    private final PaymentRepository paymentRepository;
    private final OrderRepository orderRepository;
    private final IOrderService orderService;
    private final IEmailService emailService;

    @Value("${stripe.secret.key}")
    private String stripeSecretKey;

    @Value("${stripe.webhook.secret}")
    private String stripeWebhookSecret;

    private DtoPayment dtoConverter(Payment payment) {
        DtoPayment dto = new DtoPayment();
        BeanUtils.copyProperties(payment, dto);
        dto.setOrderId(payment.getOrder().getId());
        dto.setTransactionKey(payment.getTransactionKey());
        dto.setStatus(payment.getStatus());
        return dto;
    }

    private String createStripeCheckoutSession(Order order) throws StripeException {
        Stripe.apiKey = stripeSecretKey;

        SessionCreateParams params = SessionCreateParams.builder()
                .setMode(SessionCreateParams.Mode.PAYMENT)
                .setSuccessUrl("http://localhost:4200/payment-success?orderId=" + order.getId())
                .setCancelUrl("http://localhost:4200/payment-cancel?orderId=" + order.getId())
                .setCustomerEmail(order.getUser().getEmail())
                .putMetadata("orderId", order.getId().toString())
                .addLineItem(
                        SessionCreateParams.LineItem.builder()
                                .setQuantity(1L)
                                .setPriceData(
                                        SessionCreateParams.LineItem.PriceData.builder()
                                                .setCurrency("try")
                                                .setUnitAmount(order.getGrandTotal().multiply(BigDecimal.valueOf(100)).longValue())
                                                .setProductData(SessionCreateParams.LineItem.PriceData.ProductData.builder()
                                                        .setName("Order #" + order.getId())
                                                        .build())
                                                .build())
                                .build())
                .build();

        Session session = Session.create(params);
        return session.getId();
    }

    @Override
    public DtoPayment createPayment(DtoPaymentRequest request) {
        Order order = orderRepository.findById(request.getOrderId())
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.ORDER_NOT_FOUND, request.getOrderId().toString())));

        if (order.getStatus() == OrderStatus.CANCELLED) {
            throw new BaseException(new ErrorMessage(MessageType.ORDER_ALREADY_CANCELLED, order.getId().toString()));
        }

        boolean hasPaid = paymentRepository.findByOrderId(order.getId())
                .stream().anyMatch(p -> p.getStatus() == PaymentStatus.SUCCESS);

        if (hasPaid) {
            throw new BaseException(new ErrorMessage(MessageType.PAYMENT_ALREADY_COMPLETED, request.getOrderId().toString()));
        }

        List<Payment> pendingPayments = paymentRepository.findByOrderIdAndStatus(order.getId(), PaymentStatus.PENDING);
        for (Payment p : pendingPayments) {
            p.setStatus(PaymentStatus.FAILED);
            paymentRepository.save(p);
        }

        try {
            String sessionId = createStripeCheckoutSession(order);

            Payment payment = new Payment();
            payment.setOrder(order);
            payment.setAmount(order.getGrandTotal());
            payment.setPaymentMethod(request.getPaymentMethod() != null ? request.getPaymentMethod() : PaymentMethod.CREDIT_CARD);
            payment.setTransactionKey(sessionId);
            payment.setStatus(PaymentStatus.PENDING);

            Payment savedPayment = paymentRepository.save(payment);
            return dtoConverter(savedPayment);
        } catch (StripeException e) {
            throw new RuntimeException("Stripe session creation failed", e);
        }
    }

    @Override
    public DtoPayment findPaymentByOrderId(Long orderId) {
        List<Payment> payments = paymentRepository.findByOrderId(orderId);
        if (payments.isEmpty()) {
            throw new BaseException(new ErrorMessage(MessageType.NO_PAYMENT_FOUND_FOR_THIS_ORDER, orderId.toString()));
        }

        Payment latestPayment = payments.stream()
                .max(Comparator.comparing(Payment::getCreatedAt))
                .orElseThrow();

        return dtoConverter(latestPayment);
    }

    @Override
    public List<DtoPayment> findPaymentsByUserId(Long userId) {
        List<Payment> payments = paymentRepository.findByUserId(userId);
        return payments.stream().map(this::dtoConverter).collect(Collectors.toList());
    }

    @Override
    public boolean hasSuccessfulPayment(Long orderId) {
        return paymentRepository.findByOrderId(orderId).stream()
                .anyMatch(p -> p.getStatus() == PaymentStatus.SUCCESS);
    }

    @Override
    @Transactional
    public DtoPayment updatePaymentStatus(Long orderId, PaymentStatus newStatus, String transactionKey) {
        List<Payment> payments = paymentRepository.findByOrderIdAndStatus(orderId, PaymentStatus.PENDING);
        if (payments.isEmpty()) return null;

        Payment payment = payments.get(0);
        payment.setStatus(newStatus);
        if (transactionKey != null) {
            payment.setTransactionKey(transactionKey);
        }

        if (newStatus == PaymentStatus.SUCCESS) {
            confirmAllOrdersAsPaid(payment.getOrder());
        }

        return dtoConverter(paymentRepository.save(payment));
    }

    private void confirmAllOrdersAsPaid(Order masterOrder) {
        masterOrder.setStatus(OrderStatus.PAID);
        if (masterOrder.getSubOrders() != null) {
            masterOrder.getSubOrders().forEach(sub -> sub.setStatus(OrderStatus.PAID));
        }
        orderRepository.save(masterOrder);

        emailService.sendOrderConfirmationEmail(
                masterOrder.getUser().getEmail(),
                masterOrder.getId().toString(),
                masterOrder.getGrandTotal()
        );
    }

    @Override
    public DtoPayment refundPaymentItem(Long orderId, Long orderItemId) throws StripeException {
        Order order = orderService.findEntityOrderById(orderId);

        OrderItem itemToRefund = order.getOrderItems().stream()
                .filter(item -> item.getId().equals(orderItemId))
                .findFirst()
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.ORDER_ITEM_NOT_FOUND, orderItemId.toString())));

        Order masterOrder = order.getParentOrder() != null ? order.getParentOrder() : order;

        Payment payment = paymentRepository.findByOrderIdAndStatus(masterOrder.getId(), PaymentStatus.SUCCESS)
                .stream()
                .max(Comparator.comparing(Payment::getCreatedAt))
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.NO_PAYMENT_FOUND, masterOrder.getId().toString())));

        Stripe.apiKey = stripeSecretKey;
        RefundCreateParams params = RefundCreateParams.builder()
                .setPaymentIntent(payment.getTransactionKey())
                .setAmount(itemToRefund.getPrice().multiply(BigDecimal.valueOf(100)).longValue())
                .build();
        Refund.create(params);

        orderService.processItemRefund(order, itemToRefund);

        payment.setStatus(PaymentStatus.PARTIALLY_REFUNDED);
        return dtoConverter(paymentRepository.save(payment));
    }

    @Override
    public void handleStripeWebhook(String payload, String sigHeader) {
        try {
            Stripe.apiKey = stripeSecretKey;
            Event event = Webhook.constructEvent(payload, sigHeader, stripeWebhookSecret);
            StripeObject stripeObject = event.getData().getObject();

            if (stripeObject instanceof Session session) {
                String orderIdStr = session.getMetadata().get("orderId");
                if (orderIdStr != null) {
                    updatePaymentStatus(Long.parseLong(orderIdStr), PaymentStatus.SUCCESS, session.getId());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Webhook processing failed", e);
        }
    }
}
