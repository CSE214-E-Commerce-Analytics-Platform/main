package com.furkan.services.impl;

import com.furkan.services.IEmailService;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EmailServiceImpl implements IEmailService {

    private final JavaMailSender mailSender;

    @Value("${app.mail.from}")
    private String fromEmail;

    @Value("${app.mail.base-url}")
    private String baseUrl;

    @Override
    public void sendVerificationEmail(String toEmail, String token) {
        String verifyLink = baseUrl + "/verify-email?token=" + token;

        String htmlContent = "<div style='font-family: Arial, sans-serif; padding: 20px; color: #333;'>" +
                "<h2 style='color: #4CAF50;'>Welcome to Our E-commerce Platform!</h2>" +
                "<p>To complete your registration, please verify your email address by clicking the button below:</p>" +
                "<a href='" + verifyLink + "' style='display: inline-block; padding: 10px 20px; margin: 20px 0; background-color: #4CAF50; color: white; text-decoration: none; border-radius: 5px;'>Verify My Account</a>" +
                "<p>This verification link will expire in 24 hours.</p>" +
                "<p>If you did not create an account, please ignore this email.</p>" +
                "<p>Best regards,<br>Ecommerce Team</p>" +
                "</div>";

        sendHtmlEmail(toEmail, "Action Required: Verify Your Email Address", htmlContent);
    }

    @Override
    public void sendPasswordResetEmail(String toEmail, String token) {
        String resetLink = baseUrl + "/reset-password?token=" + token;

        String htmlContent = "<div style='font-family: Arial, sans-serif; padding: 20px; color: #333;'>" +
                "<h2 style='color: #f44336;'>Password Reset Request</h2>" +
                "<p>We received a request to reset the password for your account. Click the button below to set a new password:</p>" +
                "<a href='" + resetLink + "' style='display: inline-block; padding: 10px 20px; margin: 20px 0; background-color: #f44336; color: white; text-decoration: none; border-radius: 5px;'>Reset My Password</a>" +
                "<p>This link will expire in 15 minutes.</p>" +
                "<p>If you did not request a password reset, please ignore this email or contact support if you have concerns.</p>" +
                "<p>Best regards,<br>Ecommerce Team</p>" +
                "</div>";

        sendHtmlEmail(toEmail, "Reset Your Password", htmlContent);
    }

    @Override
    public void sendAccountCreatedEmail(String toEmail, String email) {
        String loginLink = baseUrl + "/login";

        String htmlContent = "<div style='font-family: Arial, sans-serif; padding: 20px; color: #333;'>" +
                "<h2 style='color: #2196F3;'>Account Successfully Created!</h2>" +
                "<p>Hello " + email + ",</p>" +
                "<p>Your account has been successfully created. We are thrilled to have you on board!</p>" +
                "<p>You can now log in to your account and start exploring our platform.</p>" +
                "<a href='" + loginLink + "' style='display: inline-block; padding: 10px 20px; margin: 20px 0; background-color: #2196F3; color: white; text-decoration: none; border-radius: 5px;'>Log In Now</a>" +
                "<p>Best regards,<br>Ecommerce Team</p>" +
                "</div>";

        sendHtmlEmail(toEmail, "Welcome! Your Account Has Been Created", htmlContent);
    }

    @Override
    public void sendOrderConfirmationEmail(String toEmail, Long orderNumber, byte[] pdfAttachment) {
        String htmlContent = "<div style='font-family: Arial, sans-serif; padding: 20px; color: #333;'>" +
                "<h2 style='color: #2196F3;'>Your Order Has Been Successfully Placed!</h2>" +
                "<p>Hello,</p>" +
                "<p>We have successfully received your order <b>#" + orderNumber + "</b>. Thank you for shopping with us!</p>" +
                "<p>You can find the detailed summary and invoice for your order in the attached <b>PDF</b> document.</p>" +
                "<p>We will notify you again as soon as your order has been shipped.</p>" +
                "<p>Best regards,<br>Ecommerce Team</p>" +
                "</div>";

        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setFrom(fromEmail);
            helper.setTo(toEmail);
            helper.setSubject("Order Confirmation - #" + orderNumber);

            helper.setText(htmlContent, true);

            if (pdfAttachment != null && pdfAttachment.length > 0) {
                String fileName = "Order_Summary_" + orderNumber + ".pdf";
                helper.addAttachment(fileName, new ByteArrayResource(pdfAttachment));
            }

            mailSender.send(message);

        } catch (MessagingException e) {
            throw new RuntimeException("An error occurred while sending the order confirmation email", e);
        }
    }

    private void sendHtmlEmail(String toEmail, String subject, String htmlBody) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, false, "UTF-8");

            helper.setFrom(fromEmail);
            helper.setTo(toEmail);
            helper.setSubject(subject);
            helper.setText(htmlBody, true);

            mailSender.send(message);
        } catch (MessagingException e) {
            throw new RuntimeException("An error occur while sending email.", e);
        }
    }
}
