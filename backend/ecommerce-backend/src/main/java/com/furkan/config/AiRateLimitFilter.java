package com.furkan.config;

import com.furkan.entities.User;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AV-09 (Object Enumeration) saldırısına karşı rate limiting.
 * Kullanıcı başına dakikada max 10 AI isteği.
 */
@Component
public class AiRateLimitFilter extends OncePerRequestFilter {

    private static final int MAX_REQUESTS_PER_MINUTE = 10;

    // userId -> [requestCount, windowStartMs]
    private final ConcurrentHashMap<Long, long[]> requestCounts = new ConcurrentHashMap<>();

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        // Sadece /api/ai/** endpoint'lerine uygula
        if (!request.getRequestURI().startsWith("/api/ai/")) {
            filterChain.doFilter(request, response);
            return;
        }

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !(auth.getPrincipal() instanceof User currentUser)) {
            filterChain.doFilter(request, response);
            return;
        }

        Long userId = currentUser.getId();
        long now = System.currentTimeMillis();

        requestCounts.compute(userId, (id, windowData) -> {
            if (windowData == null || now - windowData[1] > 60_000L) {
                // Yeni pencere başlat
                return new long[]{1, now};
            }
            windowData[0]++;
            return windowData;
        });

        long[] windowData = requestCounts.get(userId);
        if (windowData[0] > MAX_REQUESTS_PER_MINUTE) {
            response.setStatus(429); // Too Many Requests
            response.setContentType("application/json");
            response.getWriter().write(
                "{\"error\": \"Rate limit exceeded. Max 10 requests per minute.\", \"status\": 429}"
            );
            return;
        }

        filterChain.doFilter(request, response);
    }
}
