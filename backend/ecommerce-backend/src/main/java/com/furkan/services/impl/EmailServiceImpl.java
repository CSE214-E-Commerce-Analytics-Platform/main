package com.furkan.services.impl;

import com.furkan.enums.OrderStatus;
import com.furkan.services.IEmailService;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
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

    private void sendHtmlEmail(String to, String subject, String htmlBody) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setFrom(fromEmail);
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(htmlBody, true);

            mailSender.send(message);
        } catch (Exception e) {
            System.err.println("Email sending failed to: " + to + " Error: " + e.getMessage());
        }
    }

    @Override
    @Async
    public void sendVerificationEmail(String toEmail, String token) {
        String verifyLink = baseUrl + "/verify-email?token=" + token;
        String subject = "Account Verification - E-Commerce Analytics Platform";

        String content = "<p>Welcome aboard!</p>"
                + "<p>To activate your account and start exploring, please verify your email address by clicking the button below.</p>"
                + "<div style='text-align: center; margin: 30px 0;'>"
                + "<a href='" + verifyLink + "' style='background-color: #3498db; color: #ffffff; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;'>Verify My Account</a>"
                + "</div>"
                + "<p style='font-size: 14px; color: #7f8c8d;'>This link will expire in 24 hours.</p>";

        sendHtmlEmail(toEmail, subject, buildHtmlTemplate("Verify Your Email Address", content));
    }

    @Override
    @Async
    public void sendPasswordResetEmail(String toEmail, String token) {
        String resetLink = baseUrl + "/reset-password?token=" + token;
        String subject = "Password Reset Request - E-Commerce Analytics Platform";

        String content = "<p>Hello,</p>"
                + "<p>We received a request to reset the password for your account. Click the button below to set a new password.</p>"
                + "<div style='text-align: center; margin: 30px 0;'>"
                + "<a href='" + resetLink + "' style='background-color: #e74c3c; color: #ffffff; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;'>Reset My Password</a>"
                + "</div>"
                + "<p style='font-size: 14px; color: #7f8c8d;'>If you did not request a password reset, please ignore this email. This link is valid for 15 minutes.</p>";

        sendHtmlEmail(toEmail, subject, buildHtmlTemplate("Reset Your Password", content));
    }

    @Override
    @Async
    public void sendOrderConfirmationEmail(String toEmail, String orderNumber, BigDecimal totalAmount) {
        String subject = "Order Confirmation - #" + orderNumber;

        String content = "<p>Your payment has been successfully processed and your order is confirmed.</p>"
                + "<table style='width: 100%; margin: 20px 0; border-collapse: collapse;'>"
                + "<tr><td style='padding: 10px; border-bottom: 1px solid #eee;'><b>Order Number:</b></td><td style='padding: 10px; border-bottom: 1px solid #eee; text-align: right;'>#" + orderNumber + "</td></tr>"
                + "<tr><td style='padding: 10px; border-bottom: 1px solid #eee;'><b>Total Amount:</b></td><td style='padding: 10px; border-bottom: 1px solid #eee; text-align: right; color: #27ae60; font-weight: bold;'>" + totalAmount + " TL</td></tr>"
                + "</table>"
                + "<p>We will notify you once your items are prepared and shipped. Thank you for choosing us.</p>";

        sendHtmlEmail(toEmail, subject, buildHtmlTemplate("Thank You For Your Order!", content));
    }

    @Override
    @Async
    public void sendOrderStatusUpdateEmail(String toEmail, String orderNumber, OrderStatus status) {
        String subject = "Order Status Updated - #" + orderNumber;

        String content = "<p>The status of your order <b>#" + orderNumber + "</b> has been updated.</p>"
                + "<div style='background-color: #fdf2e9; border-left: 5px solid #e67e22; padding: 15px; margin: 20px 0;'>"
                + "<p style='margin: 0; font-size: 16px;'>New Status: <strong style='color: #d35400;'>" + status.name() + "</strong></p>"
                + "</div>"
                + "<p>You can track the current details of your order from your user dashboard.</p>";

        sendHtmlEmail(toEmail, subject, buildHtmlTemplate("Order Status Update", content));
    }

    @Override
    @Async
    public void sendOrderCancellationEmail(String toEmail, String orderNumber, String reason) {
        String subject = "Order Cancelled - #" + orderNumber;

        String content = "<p>Your order <b>#" + orderNumber + "</b> has been cancelled for the following reason:</p>"
                + "<div style='background-color: #fbedec; border-left: 5px solid #c0392b; padding: 15px; margin: 20px 0;'>"
                + "<p style='margin: 0; color: #c0392b;'>" + reason + "</p>"
                + "</div>"
                + "<p>Your payment will be refunded to your card within 1-3 business days, depending on your bank. We apologize for any inconvenience caused.</p>";

        sendHtmlEmail(toEmail, subject, buildHtmlTemplate("Order Cancelled", content));
    }

    @Override
    @Async
    public void sendNewOrderNotificationToStore(String storeEmail, String orderNumber) {
        String subject = "NEW ORDER RECEIVED! - #" + orderNumber;

        String content = "<p>Congratulations! You have received a new order <b>#" + orderNumber + "</b> for your store.</p>"
                + "<p>The customer has successfully completed the payment. Please log in to your seller dashboard as soon as possible to begin preparing the items for shipment.</p>"
                + "<div style='text-align: center; margin: 30px 0;'>"
                + "<a href='" + baseUrl + "/admin/orders' style='background-color: #27ae60; color: #ffffff; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;'>View Order</a>"
                + "</div>";

        sendHtmlEmail(storeEmail, subject, buildHtmlTemplate("You Have a New Order!", content));
    }
}
