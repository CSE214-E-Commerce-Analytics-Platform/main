package com.furkan.services.impl;

import com.furkan.enums.OrderStatus;
import com.furkan.services.IEmailService;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
public class EmailServiceImpl implements IEmailService {

    private final JavaMailSender mailSender;

    @Value("${app.mail.from}")
    private String fromEmail;

    @Value("${app.mail.base-url}")
    private String baseUrl;

    private String buildHtmlTemplate(String title, String content) {
        return "<div style='font-family: \"Segoe UI\", Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f7f6; padding: 40px 20px;'>"
                + "<div style='max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 15px rgba(0,0,0,0.05);'>"
                + "<div style='background-color: #2c3e50; padding: 25px; text-align: center;'>"
                + "<h1 style='color: #ffffff; margin: 0; font-size: 24px; letter-spacing: 1px;'>E-Commerce Analytics Platform</h1>"
                + "</div>"
                + "<div style='padding: 40px 30px; color: #444444; line-height: 1.6; font-size: 16px;'>"
                + "<h2 style='color: #2c3e50; margin-top: 0;'>" + title + "</h2>"
                + content
                + "</div>"
                + "<div style='background-color: #f8f9fa; padding: 20px; text-align: center; font-size: 13px; color: #888888; border-top: 1px solid #eeeeee;'>"
                + "<p style='margin: 0;'>This email was sent automatically. Please do not reply.</p>"
                + "<p style='margin: 8px 0 0 0;'>&copy; 2026 E-Commerce Analytics Platform. All rights reserved.</p>"
                + "</div>"
                + "</div>"
                + "</div>";
    }


    private void sendHtmlEmail(String to, String subject, String htmlBody, String attachmentName, byte[] attachmentData) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setFrom(fromEmail);
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(htmlBody, true);

            if (attachmentData != null && attachmentData.length > 0) {
                helper.addAttachment(attachmentName, new ByteArrayResource(attachmentData));
            }

            mailSender.send(message);
        } catch (MessagingException e) {
            throw new RuntimeException("An error occurred while sending email to: " + to, e);
        }
    }
    
    private void sendHtmlEmail(String to, String subject, String htmlBody) {
        sendHtmlEmail(to, subject, htmlBody, null, null);
    }

    @Override
    @Async
    public void sendVerificationEmail(String toEmail, String token) {
        String verifyLink = baseUrl + "/verify-email?token=" + token;
        String content = "<p>Welcome aboard!</p>"
                + "<p>To activate your account, please verify your email address by clicking the button below:</p>"
                + "<div style='text-align: center; margin: 30px 0;'>"
                + "<a href='" + verifyLink + "' style='background-color: #3498db; color: #ffffff; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;'>Verify My Account</a>"
                + "</div>"
                + "<p style='font-size: 14px; color: #7f8c8d;'>This link will expire in 24 hours.</p>";

        sendHtmlEmail(toEmail, "Account Verification Required", buildHtmlTemplate("Verify Your Email", content));
    }

    @Override
    @Async
    public void sendPasswordResetEmail(String toEmail, String token) {
        String resetLink = baseUrl + "/reset-password?token=" + token;
        String content = "<p>We received a request to reset your password. Click the button below to set a new one:</p>"
                + "<div style='text-align: center; margin: 30px 0;'>"
                + "<a href='" + resetLink + "' style='background-color: #e74c3c; color: #ffffff; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;'>Reset My Password</a>"
                + "</div>"
                + "<p style='font-size: 14px; color: #7f8c8d;'>Valid for 15 minutes.</p>";

        sendHtmlEmail(toEmail, "Password Reset Request", buildHtmlTemplate("Reset Your Password", content));
    }

    @Override
    @Async
    public void sendOrderConfirmationEmail(String toEmail, String orderNumber, BigDecimal totalAmount) {
        String content = "<p>Your payment was successful and your order is confirmed.</p>"
                + "<table style='width: 100%; margin: 20px 0;'>"
                + "<tr><td><b>Order:</b></td><td style='text-align: right;'>#" + orderNumber + "</td></tr>"
                + "<tr><td><b>Total:</b></td><td style='text-align: right; color: #27ae60; font-weight: bold;'>" + totalAmount + " TL</td></tr>"
                + "</table>";

        sendHtmlEmail(toEmail, "Order Confirmation - #" + orderNumber, buildHtmlTemplate("Thank You!", content));
    }

    @Override
    @Async
    public void sendOrderConfirmationEmail(String toEmail, Long orderNumber, byte[] pdfAttachment) {
        String content = "<p>Your order <b>#" + orderNumber + "</b> has been placed successfully.</p>"
                + "<p>You can find your invoice in the attached <b>PDF</b> document.</p>";

        String fileName = "Invoice_" + orderNumber + ".pdf";
        sendHtmlEmail(toEmail, "Order Summary - #" + orderNumber, buildHtmlTemplate("Order Details", content), fileName, pdfAttachment);
    }

    @Override
    @Async
    public void sendOrderStatusUpdateEmail(String toEmail, String orderNumber, OrderStatus status) {
        String content = "<p>The status of your order <b>#" + orderNumber + "</b> has changed:</p>"
                + "<div style='background-color: #fdf2e9; padding: 15px; border-left: 5px solid #e67e22;'>"
                + "New Status: <b>" + status.name() + "</b></div>";

        sendHtmlEmail(toEmail, "Order Status Updated", buildHtmlTemplate("Status Update", content));
    }

    @Override
    @Async
    public void sendOrderCancellationEmail(String toEmail, String orderNumber, String reason) {
        String content = "<p>Order <b>#" + orderNumber + "</b> was cancelled.</p>"
                + "<p>Reason: <i style='color: #c0392b;'>" + reason + "</i></p>";

        sendHtmlEmail(toEmail, "Order Cancelled", buildHtmlTemplate("Cancellation Notice", content));
    }

    @Override
    @Async
    public void sendNewOrderNotificationToStore(String storeEmail, String orderNumber) {
        String content = "<p>You have received a new order <b>#" + orderNumber + "</b>.</p>"
                + "<div style='text-align: center; margin: 30px 0;'>"
                + "<a href='" + baseUrl + "/admin/orders' style='background-color: #27ae60; color: #ffffff; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: bold;'>View Order</a>"
                + "</div>";

        sendHtmlEmail(storeEmail, "NEW ORDER! - #" + orderNumber, buildHtmlTemplate("New Order Received", content));
    }

    @Override
    public void sendAccountCreatedEmail(String toEmail, String email) {
        String content = "<p>Hello " + email + ", your account has been successfully created.</p>";
        sendHtmlEmail(toEmail, "Welcome!", buildHtmlTemplate("Account Created", content));
    }
}