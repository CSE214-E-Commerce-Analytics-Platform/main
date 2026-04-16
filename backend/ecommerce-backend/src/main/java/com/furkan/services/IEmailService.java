package com.furkan.services;


public interface IEmailService {

    void sendVerificationEmail(String toEmail, String token);

    void sendPasswordResetEmail(String toEmail, String token);

    void sendAccountCreatedEmail(String toEmail, String email);

    void sendOrderConfirmationEmail(String toEmail, Long orderNumber, byte[] pdfAttachment);
}
