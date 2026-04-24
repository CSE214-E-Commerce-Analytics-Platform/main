package com.furkan.services.impl;

import com.furkan.entities.OrderItem;
import com.furkan.services.IEmailService;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
@RequiredArgsConstructor
public class EmailServiceImpl implements IEmailService {
    private final JavaMailSender mailSender;

    @Value("${app.mail.from}")
    private String fromEmail;

    @Value("${app.mail.base-url}")
    private String baseUrl;

    // ─── CORE HTML BUILDER ────────────────────────────────────────────────────

    private String buildBase(String content) {
        return """
            <html><body style="margin:0;padding:0;background:#f0f2f5;font-family:'Segoe UI',Arial,sans-serif;">
            <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f2f5;padding:40px 0;">
              <tr><td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
                  <!-- HEADER -->
                  <tr>
                    <td style="background:linear-gradient(135deg,#1a1a2e 0%,#16213e 50%,#0f3460 100%);padding:36px 40px;text-align:center;">
                      <h1 style="color:#ffffff;margin:0;font-size:26px;letter-spacing:2px;font-weight:700;">🛒 ShopFlow</h1>
                      <p style="color:#a0b4cc;margin:6px 0 0;font-size:13px;letter-spacing:1px;">E-COMMERCE PLATFORM</p>
                    </td>
                  </tr>
                  <!-- CONTENT -->
                  <tr><td style="padding:40px;">
                    %s
                  </td></tr>
                  <!-- FOOTER -->
                  <tr>
                    <td style="background:#f8f9fa;padding:24px 40px;text-align:center;border-top:1px solid #e9ecef;">
                      <p style="color:#6c757d;font-size:12px;margin:0;">This email was sent automatically. Please do not reply.</p>
                      <p style="color:#adb5bd;font-size:11px;margin:8px 0 0;">© 2026 ShopFlow. All rights reserved.</p>
                    </td>
                  </tr>
                </table>
              </td></tr>
            </table>
            </body></html>
        """.formatted(content);
    }

    private String buildButton(String url, String text, String color) {
        return """
            <div style="text-align:center;margin:32px 0;">
              <a href="%s" style="background:%s;color:#ffffff;padding:15px 36px;text-decoration:none;
                border-radius:8px;font-weight:700;font-size:15px;display:inline-block;
                letter-spacing:0.5px;box-shadow:0 4px 12px rgba(0,0,0,0.15);">%s</a>
            </div>
        """.formatted(url, color, text);
    }

    private String buildOrderItemsTable(List<OrderItem> items) {
        StringBuilder sb = new StringBuilder();
        sb.append("""
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="border-collapse:collapse;margin:20px 0;border-radius:8px;overflow:hidden;border:1px solid #e9ecef;">
              <tr style="background:#f8f9fa;">
                <th style="padding:12px 16px;text-align:left;font-size:13px;color:#495057;font-weight:600;">Product</th>
                <th style="padding:12px 16px;text-align:center;font-size:13px;color:#495057;font-weight:600;">Qty</th>
                <th style="padding:12px 16px;text-align:right;font-size:13px;color:#495057;font-weight:600;">Price</th>
              </tr>
        """);

        for (int i = 0; i < items.size(); i++) {
            OrderItem item = items.get(i);
            String bg = i % 2 == 0 ? "#ffffff" : "#fafbfc";
            BigDecimal subtotal = item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity()));
            sb.append("""
                <tr style="background:%s;">
                  <td style="padding:12px 16px;font-size:14px;color:#343a40;">%s</td>
                  <td style="padding:12px 16px;font-size:14px;color:#343a40;text-align:center;">%d</td>
                  <td style="padding:12px 16px;font-size:14px;color:#343a40;text-align:right;">%.2f ₺</td>
                </tr>
            """.formatted(bg, item.getProduct().getName(), item.getQuantity(), subtotal));
        }

        sb.append("</table>");
        return sb.toString();
    }

    private String buildInfoRow(String label, String value) {
        return """
            <tr>
              <td style="padding:10px 0;color:#6c757d;font-size:14px;border-bottom:1px solid #f1f3f4;">%s</td>
              <td style="padding:10px 0;color:#212529;font-size:14px;font-weight:600;text-align:right;border-bottom:1px solid #f1f3f4;">%s</td>
            </tr>
        """.formatted(label, value);
    }

    private String buildAlert(String text, String bgColor, String borderColor) {
        return """
            <div style="background:%s;border-left:4px solid %s;padding:16px 20px;border-radius:6px;margin:20px 0;">
              <p style="margin:0;color:#212529;font-size:14px;">%s</p>
            </div>
        """.formatted(bgColor, borderColor, text);
    }

    // ─── EMAIL SENDER ──────────────────────────────────────────────────────────

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

    // ─── AUTH EMAILS ───────────────────────────────────────────────────────────

    @Override
    @Async
    public void sendVerificationEmail(String toEmail, String token) {
        String link = baseUrl + "/verify-email?token=" + token;
        String content = """
            <h2 style="color:#1a1a2e;margin:0 0 16px;">Verify Your Email Address</h2>
            <p style="color:#495057;font-size:15px;line-height:1.7;">Welcome! You're almost there. Click the button below to verify your email and activate your account.</p>
            %s
            %s
            <p style="color:#6c757d;font-size:13px;margin-top:24px;">⏱ This link expires in <b>24 hours</b>. If you didn't create an account, you can safely ignore this email.</p>
        """.formatted(
                buildButton(link, "✓ Verify My Account", "#0f3460"),
                buildAlert("If the button doesn't work, copy and paste this link: <a href='" + link + "' style='color:#0f3460;'>" + link + "</a>", "#e8f4fd", "#3498db")
        );
        send(toEmail, "Verify your ShopFlow account", buildBase(content));
    }

    @Override
    @Async
    public void sendPasswordResetEmail(String toEmail, String token) {
        String link = baseUrl + "/reset-password?token=" + token;
        String content = """
            <h2 style="color:#1a1a2e;margin:0 0 16px;">Reset Your Password</h2>
            <p style="color:#495057;font-size:15px;line-height:1.7;">We received a request to reset your password. Click the button below to set a new one.</p>
            %s
            %s
            <p style="color:#6c757d;font-size:13px;margin-top:24px;">⏱ This link expires in <b>15 minutes</b>. If you didn't request a password reset, please ignore this email.</p>
        """.formatted(
                buildButton(link, "🔐 Reset My Password", "#e74c3c"),
                buildAlert("⚠️ For security, never share this link with anyone.", "#fff3cd", "#f0ad4e")
        );
        send(toEmail, "Password Reset Request - ShopFlow", buildBase(content));
    }

    @Override
    @Async
    public void sendWelcomeEmail(String toEmail) {
        String content = """
            <h2 style="color:#1a1a2e;margin:0 0 16px;">Welcome to ShopFlow, %s! 🎉</h2>
            <p style="color:#495057;font-size:15px;line-height:1.7;">Your account is ready. Start exploring thousands of products from verified stores.</p>
            %s
            <table width="100%%" cellpadding="0" cellspacing="0" style="margin:24px 0;">
              <tr>
                <td width="33%%" style="text-align:center;padding:16px;">
                  <div style="font-size:28px;">🛍️</div>
                  <p style="color:#495057;font-size:13px;margin:8px 0 0;">Browse Products</p>
                </td>
                <td width="33%%" style="text-align:center;padding:16px;">
                  <div style="font-size:28px;">🏪</div>
                  <p style="color:#495057;font-size:13px;margin:8px 0 0;">Discover Stores</p>
                </td>
                <td width="33%%" style="text-align:center;padding:16px;">
                  <div style="font-size:28px;">⭐</div>
                  <p style="color:#495057;font-size:13px;margin:8px 0 0;">Leave Reviews</p>
                </td>
              </tr>
            </table>
        """.formatted(toEmail, buildButton(baseUrl + "/individual/products", "🚀 Start Shopping", "#27ae60"));
        send(toEmail, "Welcome to ShopFlow! 🎉", buildBase(content));
    }

    // ─── ORDER EMAILS ──────────────────────────────────────────────────────────

    @Override
    @Async
    public void sendOrderConfirmationEmail(String toEmail, String orderNumber,
                                           BigDecimal totalAmount, List<OrderItem> items) {
        String content = """
            <h2 style="color:#1a1a2e;margin:0 0 8px;">Order Confirmed! ✅</h2>
            <p style="color:#495057;font-size:15px;line-height:1.7;">Thank you for your purchase. Your order has been received and is being processed.</p>
            %s
            <table width="100%%" cellpadding="0" cellspacing="0" style="margin:20px 0;">
              %s
              %s
            </table>
            %s
        """.formatted(
                buildOrderItemsTable(items),
                buildInfoRow("Order Number", "#" + orderNumber),
                buildInfoRow("Total Amount", String.format("%.2f ₺", totalAmount)),
                buildAlert("📦 We'll notify you when your order is shipped with tracking information.", "#e8f5e9", "#27ae60")
        );
        send(toEmail, "Order Confirmed #" + orderNumber + " - ShopFlow", buildBase(content));
    }

    @Override
    @Async
    public void sendOrderCancellationEmail(String toEmail, String orderNumber, String reason) {
        String content = """
            <h2 style="color:#1a1a2e;margin:0 0 16px;">Order Cancelled</h2>
            <p style="color:#495057;font-size:15px;line-height:1.7;">Your order <b>#%s</b> has been cancelled.</p>
            %s
            %s
        """.formatted(
                orderNumber,
                buildAlert("Reason: " + reason, "#fdf0f0", "#e74c3c"),
                buildAlert("If you paid for this order, a refund will be processed within 3–5 business days.", "#e8f4fd", "#3498db")
        );
        send(toEmail, "Order Cancelled #" + orderNumber + " - ShopFlow", buildBase(content));
    }

    // ─── SHIPMENT EMAILS ───────────────────────────────────────────────────────

    @Override
    @Async
    public void sendShipmentCreatedEmail(String toEmail, String orderNumber,
                                         String trackingNumber, String cargoFirm) {
        String content = """
            <h2 style="color:#1a1a2e;margin:0 0 16px;">Your Order is On Its Way! 🚚</h2>
            <p style="color:#495057;font-size:15px;line-height:1.7;">Great news! Your order <b>#%s</b> has been shipped.</p>
            <table width="100%%" cellpadding="0" cellspacing="0" style="margin:20px 0;">
              %s
              %s
            </table>
            %s
        """.formatted(
                orderNumber,
                buildInfoRow("Cargo Firm", cargoFirm),
                buildInfoRow("Tracking Number", "<span style='font-family:monospace;background:#f8f9fa;padding:2px 8px;border-radius:4px;'>" + trackingNumber + "</span>"),
                buildAlert("You can track your shipment using the tracking number above on the cargo firm's website.", "#e8f5e9", "#27ae60")
        );
        send(toEmail, "Your Order Has Shipped! #" + orderNumber + " - ShopFlow", buildBase(content));
    }

    @Override
    @Async
    public void sendOrderDeliveredEmail(String toEmail, String orderNumber) {
        String content = """
            <h2 style="color:#1a1a2e;margin:0 0 16px;">Order Delivered! 🎉</h2>
            <p style="color:#495057;font-size:15px;line-height:1.7;">Your order <b>#%s</b> has been delivered. We hope you love your purchase!</p>
            %s
            %s
        """.formatted(
                orderNumber,
                buildButton(baseUrl + "/individual/orders", "⭐ Leave a Review", "#f39c12"),
                buildAlert("If you have any issues with your order, please contact our support team.", "#e8f4fd", "#3498db")
        );
        send(toEmail, "Order Delivered #" + orderNumber + " - ShopFlow", buildBase(content));
    }

    // ─── STORE EMAILS ──────────────────────────────────────────────────────────

    @Override
    @Async
    public void sendNewOrderNotificationToStore(String storeEmail, String orderNumber,
                                                List<OrderItem> items) {
        String content = """
            <h2 style="color:#1a1a2e;margin:0 0 16px;">New Order Received! 🛍️</h2>
            <p style="color:#495057;font-size:15px;line-height:1.7;">You have a new order <b>#%s</b> waiting to be processed.</p>
            %s
            %s
        """.formatted(
                orderNumber,
                buildOrderItemsTable(items),
                buildButton(baseUrl + "/corporate/orders", "📦 Process Order", "#0f3460")
        );
        send(storeEmail, "New Order #" + orderNumber + " - ShopFlow", buildBase(content));
    }
}