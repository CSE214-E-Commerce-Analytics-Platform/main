package com.furkan.services;


import com.furkan.entities.OrderItem;

import java.math.BigDecimal;
import java.util.List;

public interface IEmailService {

    // --- AUTH ---
    void sendVerificationEmail(String toEmail, String token);
    void sendPasswordResetEmail(String toEmail, String token);
    void sendWelcomeEmail(String toEmail);

    // --- Order ---
    void sendOrderConfirmationEmail(String toEmail, String orderNumber, BigDecimal totalAmount, List<OrderItem> items);
    void sendOrderCancellationEmail(String toEmail, String orderNumber, String reason);

    // --- Shipment ---
    void sendShipmentCreatedEmail(String toEmail, String orderNumber, String trackingNumber, String cargoFirm);
    void sendOrderDeliveredEmail(String toEmail, String orderNumber);

    // --- Store ---
    void sendNewOrderNotificationToStore(String storeEmail, String orderNumber, List<OrderItem> items);
}
