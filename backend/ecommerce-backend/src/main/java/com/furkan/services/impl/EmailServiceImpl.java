package com.furkan.services.impl;

import com.furkan.services.IEmailService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
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

        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(fromEmail);
        message.setTo(toEmail);
        message.setSubject("Verify Your Email Address");
        message.setText(
                "Hello, \n\n" +
                "Please verify your email address by clicking the link bellow:\n\n" +
                verifyLink + "\n\n" +
                "This link will expire in 24 hours.\n\n" +
                "If you did not create an account, please igore this email.\n\n" +
                "Best regards,\nEcommerce Team"
        );

        mailSender.send(message);
    }

    @Override
    public void sendPasswordResetEmail(String toEmail, String token) {
        String resetLink = baseUrl + "/reset-password?token=" + token;

        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(fromEmail);
        message.setTo(toEmail);
        message.setSubject("Reset Your Password");
        message.setText(
                "Hello,\n\n" +
                        "We received a request to reset your password. Click the link below:\n\n" +
                        resetLink + "\n\n" +
                        "This link will expire in 15 minutes.\n\n" +
                        "If you did not request a password reset, please ignore this email.\n\n" +
                        "Best regards,\nEcommerce Team"
        );

        mailSender.send(message);
    }
}
