package com.furkan.services;


import com.furkan.enums.OrderStatus;

import java.math.BigDecimal;

public interface IEmailService {

    // --- AUTH ---
    void sendVerificationEmail(String toEmail, String token);

    void sendPasswordResetEmail(String toEmail, String token);

    // --- Order ---
    void sendOrderConfirmationEmail(String toEmail, String orderNumber, BigDecimal totalAmount);

    void sendOrderStatusUpdateEmail(String toEmail, String orderNumber, OrderStatus status);

    void sendOrderCancellationEmail(String toEmail, String orderNumber, String reason);

    void sendNewOrderNotificationToStore(String storeEmail, String orderNumber);
    void sendAccountCreatedEmail(String toEmail, String email);

    void sendOrderConfirmationEmail(String toEmail, Long orderNumber, byte[] pdfAttachment);
}
