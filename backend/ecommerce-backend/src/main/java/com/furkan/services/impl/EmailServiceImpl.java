package com.furkan.services.impl;

import com.furkan.entities.OrderItem;
import com.furkan.services.IEmailService;
import jakarta.annotation.PostConstruct;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmailServiceImpl implements IEmailService {

    private final JavaMailSender mailSender;

    @Value("${app.mail.from}")
    private String fromEmail;

    @Value("${app.mail.base-url}")
    private String baseUrl;

    private String logoDataUri = "";

    // Logo dosyasını uygulama başlarken bir kez okuyup base64'e çevir
    @PostConstruct
    public void loadLogo() {
        try {
            ClassPathResource resource = new ClassPathResource("static/logo.webp");
            byte[] bytes = resource.getInputStream().readAllBytes();
            String base64 = Base64.getEncoder().encodeToString(bytes);
            logoDataUri = "data:image/webp;base64," + base64;
        } catch (Exception e) {
            log.warn("Logo could not be loaded: {}", e.getMessage());
            logoDataUri = "";
        }
    }

    // ─── CORE HTML BUILDERS ───────────────────────────────────────────────────

    private String buildBase(String content) {
        String logoHtml = logoDataUri.isEmpty()
                ? "<h2 style=\"color:#4f46e5;margin:0;font-size:22px;font-weight:800;\">ShopFlow</h2>"
                : "<img src=\"" + logoDataUri + "\" alt=\"ShopFlow\" style=\"height:48px;width:auto;\" />";

        return "<!DOCTYPE html>"
                + "<html lang=\"en\"><head><meta charset=\"UTF-8\">"
                + "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1.0\">"
                + "<style>"
                + "body{margin:0;padding:0;background:#f4f6f9;font-family:'Segoe UI',Tahoma,Arial,sans-serif;}"
                + "table{border-collapse:collapse;mso-table-lspace:0;mso-table-rspace:0;}"
                + ".wrapper{width:100%;background:#f4f6f9;padding:32px 0;}"
                + ".container{width:600px;max-width:100%;margin:0 auto;background:#ffffff;"
                + "border-radius:16px;overflow:hidden;box-shadow:0 2px 20px rgba(0,0,0,0.08);}"
                + ".header{background:#ffffff;padding:24px 40px;text-align:center;border-bottom:2px solid #f0f0f0;}"
                + ".body-content{padding:40px;color:#374151;}"
                + ".footer-content{background:#f9fafb;padding:24px 40px;text-align:center;border-top:1px solid #eef0f3;}"
                + ".divider{height:1px;background:#eef0f3;margin:28px 0;border:none;}"
                + ".btn{display:inline-block;padding:14px 32px;border-radius:8px;font-weight:700;"
                + "font-size:15px;text-decoration:none;letter-spacing:0.3px;"
                + "box-shadow:0 2px 8px rgba(0,0,0,0.12);color:#ffffff !important;}"
                + ".alert{padding:14px 18px;border-radius:8px;margin:20px 0;font-size:14px;line-height:1.6;}"
                + ".info-table{width:100%;border-collapse:collapse;margin:20px 0;"
                + "border:1px solid #eef0f3;border-radius:8px;overflow:hidden;}"
                + ".info-table td{padding:12px 16px;font-size:14px;border-bottom:1px solid #eef0f3;}"
                + ".info-table tr:last-child td{border-bottom:none;}"
                + ".items-table{width:100%;border-collapse:collapse;margin:20px 0;border:1px solid #eef0f3;}"
                + ".items-table th{padding:12px 16px;background:#f9fafb;font-size:13px;"
                + "color:#6b7280;font-weight:600;border-bottom:1px solid #eef0f3;}"
                + ".items-table td{padding:12px 16px;font-size:14px;color:#374151;border-bottom:1px solid #f3f4f6;}"
                + ".items-table tr:last-child td{border-bottom:none;}"
                + ".items-table tr:nth-child(even) td{background:#fafbfc;}"
                + "@media only screen and (max-width:620px){"
                + ".container{width:100% !important;border-radius:0 !important;}"
                + ".body-content{padding:24px 16px !important;}"
                + ".header{padding:20px 16px !important;}"
                + ".footer-content{padding:20px 16px !important;}"
                + ".btn{display:block !important;text-align:center !important;}"
                + ".info-table td,.items-table th,.items-table td{padding:10px 12px !important;}"
                + "}"
                + "</style></head>"
                + "<body><div class=\"wrapper\">"
                + "<table class=\"container\" width=\"600\" cellpadding=\"0\" cellspacing=\"0\" align=\"center\">"
                + "<tr><td class=\"header\">" + logoHtml + "</td></tr>"
                + "<tr><td class=\"body-content\">" + content + "</td></tr>"
                + "<tr><td class=\"footer-content\">"
                + "<p style=\"color:#9ca3af;font-size:12px;margin:0 0 4px;\">This email was sent automatically. Please do not reply.</p>"
                + "<p style=\"color:#d1d5db;font-size:11px;margin:0;\">&#169; 2026 ShopFlow. All rights reserved.</p>"
                + "</td></tr>"
                + "</table>"
                + "</div></body></html>";
    }

    private String buildButton(String url, String text, String color) {
        return "<div style=\"text-align:center;margin:28px 0;\">"
                + "<a href=\"" + url + "\" class=\"btn\" style=\"background:" + color + ";\">"
                + text + "</a>"
                + "</div>";
    }

    private String buildDivider() {
        return "<hr class=\"divider\" />";
    }

    private String buildAlert(String text, String bgColor, String borderColor, String textColor) {
        return "<div class=\"alert\" style=\"background:" + bgColor + ";border-left:4px solid "
                + borderColor + ";color:" + textColor + ";\">"
                + text
                + "</div>";
    }

    private String buildInfoTable(String... rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("<table class=\"info-table\">");
        for (String row : rows) sb.append(row);
        sb.append("</table>");
        return sb.toString();
    }

    private String buildInfoRow(String label, String value) {
        return "<tr>"
                + "<td style=\"color:#6b7280;font-weight:500;width:40%;\">" + label + "</td>"
                + "<td style=\"color:#111827;font-weight:600;text-align:right;\">" + value + "</td>"
                + "</tr>";
    }

    private String buildOrderItemsTable(List<OrderItem> items) {
        StringBuilder sb = new StringBuilder();
        sb.append("<table class=\"items-table\">")
                .append("<tr>")
                .append("<th style=\"text-align:left;\">Product</th>")
                .append("<th style=\"text-align:center;\">Qty</th>")
                .append("<th style=\"text-align:right;\">Price</th>")
                .append("</tr>");

        for (OrderItem item : items) {
            BigDecimal subtotal = item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity()));
            sb.append("<tr>")
                    .append("<td>").append(item.getProduct().getName()).append("</td>")
                    .append("<td style=\"text-align:center;\">").append(item.getQuantity()).append("</td>")
                    .append("<td style=\"text-align:right;\">").append(String.format("%.2f TL", subtotal)).append("</td>")
                    .append("</tr>");
        }

        sb.append("</table>");
        return sb.toString();
    }

    // ─── EMAIL SENDER ─────────────────────────────────────────────────────────

    private void send(String to, String subject, String html) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");
            helper.setFrom(fromEmail);
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(html, true);
            mailSender.send(message);
        } catch (MessagingException e) {
            throw new RuntimeException("Failed to send email to: " + to, e);
        }
    }

    // ─── AUTH EMAILS ──────────────────────────────────────────────────────────

    @Override
    @Async
    public void sendVerificationEmail(String toEmail, String token) {
        String link = baseUrl + "/verify-email?token=" + token;
        String content = "<h2 style=\"color:#111827;margin:0 0 8px;font-size:22px;\">Verify Your Email Address</h2>"
                + "<p style=\"color:#6b7280;font-size:15px;line-height:1.7;margin:0 0 4px;\">Welcome! You're almost there. "
                + "Click the button below to verify your email and activate your account.</p>"
                + buildDivider()
                + buildButton(link, "Verify My Account", "#4f46e5")
                + buildDivider()
                + buildAlert("If the button doesn't work, copy and paste this link into your browser:<br>"
                        + "<a href=\"" + link + "\" style=\"color:#4f46e5;word-break:break-all;\">" + link + "</a>",
                "#f5f3ff", "#4f46e5", "#374151")
                + "<p style=\"color:#9ca3af;font-size:13px;margin:16px 0 0;\">This link expires in <b>24 hours</b>. "
                + "If you didn't create an account, you can safely ignore this email.</p>";
        send(toEmail, "Verify your ShopFlow account", buildBase(content));
    }

    @Override
    @Async
    public void sendPasswordResetEmail(String toEmail, String token) {
        String link = baseUrl + "/reset-password?token=" + token;
        String content = "<h2 style=\"color:#111827;margin:0 0 8px;font-size:22px;\">Reset Your Password</h2>"
                + "<p style=\"color:#6b7280;font-size:15px;line-height:1.7;margin:0;\">We received a request to reset your password. "
                + "Click the button below to set a new one.</p>"
                + buildDivider()
                + buildButton(link, "Reset My Password", "#dc2626")
                + buildDivider()
                + buildAlert("&#9888; For your security, never share this link with anyone.",
                "#fff7ed", "#f97316", "#92400e")
                + "<p style=\"color:#9ca3af;font-size:13px;margin:16px 0 0;\">This link expires in <b>15 minutes</b>. "
                + "If you didn't request a password reset, please ignore this email.</p>";
        send(toEmail, "Password Reset Request - ShopFlow", buildBase(content));
    }

    @Override
    @Async
    public void sendWelcomeEmail(String toEmail) {
        String content = "<h2 style=\"color:#111827;margin:0 0 8px;font-size:22px;\">Welcome to ShopFlow! &#127881;</h2>"
                + "<p style=\"color:#6b7280;font-size:15px;line-height:1.7;margin:0;\">Your account is ready. "
                + "Start exploring thousands of products from verified stores.</p>"
                + buildDivider()
                + buildButton(baseUrl + "/individual/products", "Start Shopping", "#4f46e5")
                + buildDivider()
                + "<table width=\"100%\" cellpadding=\"0\" cellspacing=\"0\">"
                + "<tr>"
                + "<td width=\"33%\" style=\"text-align:center;padding:16px 8px;\">"
                + "<div style=\"font-size:32px;margin-bottom:8px;\">&#128717;</div>"
                + "<p style=\"color:#374151;font-size:13px;font-weight:600;margin:0;\">Browse Products</p>"
                + "<p style=\"color:#9ca3af;font-size:12px;margin:4px 0 0;\">Thousands of items</p>"
                + "</td>"
                + "<td width=\"33%\" style=\"text-align:center;padding:16px 8px;\">"
                + "<div style=\"font-size:32px;margin-bottom:8px;\">&#127978;</div>"
                + "<p style=\"color:#374151;font-size:13px;font-weight:600;margin:0;\">Discover Stores</p>"
                + "<p style=\"color:#9ca3af;font-size:12px;margin:4px 0 0;\">Verified sellers</p>"
                + "</td>"
                + "<td width=\"33%\" style=\"text-align:center;padding:16px 8px;\">"
                + "<div style=\"font-size:32px;margin-bottom:8px;\">&#11088;</div>"
                + "<p style=\"color:#374151;font-size:13px;font-weight:600;margin:0;\">Leave Reviews</p>"
                + "<p style=\"color:#9ca3af;font-size:12px;margin:4px 0 0;\">Share your experience</p>"
                + "</td>"
                + "</tr>"
                + "</table>";
        send(toEmail, "Welcome to ShopFlow!", buildBase(content));
    }

    // ─── ORDER EMAILS ─────────────────────────────────────────────────────────

    @Override
    @Async
    public void sendOrderConfirmationEmail(String toEmail, String orderNumber,
                                           BigDecimal totalAmount, List<OrderItem> items) {
        String content = "<h2 style=\"color:#111827;margin:0 0 8px;font-size:22px;\">Order Confirmed! &#9989;</h2>"
                + "<p style=\"color:#6b7280;font-size:15px;line-height:1.7;margin:0;\">Thank you for your purchase! "
                + "Your order has been received and is being processed.</p>"
                + buildDivider()
                + "<p style=\"color:#374151;font-size:15px;font-weight:600;margin:0 0 12px;\">Order Summary</p>"
                + buildOrderItemsTable(items)
                + buildDivider()
                + buildInfoTable(
                buildInfoRow("Order Number", "#" + orderNumber),
                buildInfoRow("Total Amount", String.format("%.2f TL", totalAmount)))
                + buildDivider()
                + buildAlert("&#128666; We will notify you when your order is shipped with tracking information.",
                "#f0fdf4", "#22c55e", "#166534");
        send(toEmail, "Order Confirmed #" + orderNumber + " - ShopFlow", buildBase(content));
    }

    @Override
    @Async
    public void sendOrderCancellationEmail(String toEmail, String orderNumber, String reason) {
        String content = "<h2 style=\"color:#111827;margin:0 0 8px;font-size:22px;\">Order Cancelled</h2>"
                + "<p style=\"color:#6b7280;font-size:15px;line-height:1.7;margin:0;\">Your order "
                + "<b style=\"color:#111827;\">#" + orderNumber + "</b> has been cancelled.</p>"
                + buildDivider()
                + buildAlert("Reason: " + reason, "#fef2f2", "#ef4444", "#991b1b")
                + buildAlert("If you paid for this order, a refund will be processed within 3-5 business days.",
                "#eff6ff", "#3b82f6", "#1e40af");
        send(toEmail, "Order Cancelled #" + orderNumber + " - ShopFlow", buildBase(content));
    }

    // ─── SHIPMENT EMAILS ──────────────────────────────────────────────────────

    @Override
    @Async
    public void sendShipmentCreatedEmail(String toEmail, String orderNumber,
                                         String trackingNumber, String cargoFirm) {
        String content = "<h2 style=\"color:#111827;margin:0 0 8px;font-size:22px;\">Your Order is On Its Way! &#128666;</h2>"
                + "<p style=\"color:#6b7280;font-size:15px;line-height:1.7;margin:0;\">Great news! Your order "
                + "<b style=\"color:#111827;\">#" + orderNumber + "</b> has been shipped.</p>"
                + buildDivider()
                + "<p style=\"color:#374151;font-size:15px;font-weight:600;margin:0 0 12px;\">Shipment Details</p>"
                + buildInfoTable(
                buildInfoRow("Cargo Firm", cargoFirm),
                buildInfoRow("Tracking Number",
                        "<span style=\"font-family:monospace;background:#f3f4f6;padding:3px 10px;"
                                + "border-radius:6px;font-size:14px;color:#111827;\">" + trackingNumber + "</span>"))
                + buildDivider()
                + buildAlert("You can track your shipment using the tracking number above on the cargo firm's website.",
                "#f0fdf4", "#22c55e", "#166534");
        send(toEmail, "Your Order Has Shipped! #" + orderNumber + " - ShopFlow", buildBase(content));
    }

    @Override
    @Async
    public void sendOrderDeliveredEmail(String toEmail, String orderNumber) {
        String content = "<h2 style=\"color:#111827;margin:0 0 8px;font-size:22px;\">Order Delivered! &#127881;</h2>"
                + "<p style=\"color:#6b7280;font-size:15px;line-height:1.7;margin:0;\">Your order "
                + "<b style=\"color:#111827;\">#" + orderNumber + "</b> has been delivered. "
                + "We hope you love your purchase!</p>"
                + buildDivider()
                + buildButton(baseUrl + "/individual/orders", "Leave a Review", "#f59e0b")
                + buildDivider()
                + buildAlert("If you have any issues with your order, please contact our support team.",
                "#eff6ff", "#3b82f6", "#1e40af");
        send(toEmail, "Order Delivered #" + orderNumber + " - ShopFlow", buildBase(content));
    }

    // ─── STORE EMAILS ─────────────────────────────────────────────────────────

    @Override
    @Async
    public void sendNewOrderNotificationToStore(String storeEmail, String orderNumber,
                                                List<OrderItem> items) {
        String content = "<h2 style=\"color:#111827;margin:0 0 8px;font-size:22px;\">New Order Received! &#128717;</h2>"
                + "<p style=\"color:#6b7280;font-size:15px;line-height:1.7;margin:0;\">You have a new order "
                + "<b style=\"color:#111827;\">#" + orderNumber + "</b> waiting to be processed.</p>"
                + buildDivider()
                + "<p style=\"color:#374151;font-size:15px;font-weight:600;margin:0 0 12px;\">Order Items</p>"
                + buildOrderItemsTable(items)
                + buildDivider()
                + buildButton(baseUrl + "/corporate/orders", "Process Order", "#4f46e5");
        send(storeEmail, "New Order #" + orderNumber + " - ShopFlow", buildBase(content));
    }
}