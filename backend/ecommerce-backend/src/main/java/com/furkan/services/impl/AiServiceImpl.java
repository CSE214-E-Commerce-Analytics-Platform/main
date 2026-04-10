package com.furkan.services.impl;

import com.furkan.entities.Store;
import com.furkan.entities.User;
import com.furkan.services.IAiService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class AiServiceImpl implements IAiService {

    private final RestTemplate restTemplate;

    @Value("${ai.agent.url}")
    private String agentUrl;

    @Override
    public String askAi(String userQuestion, Authentication authentication) {
        // JWT'den doğrulanmış kullanıcı bilgilerini al — frontend'den asla alma!
        User currentUser = (User) authentication.getPrincipal();

        Long userId = currentUser.getId();
        String role = currentUser.getRoleType().name(); // INDIVIDUAL, CORPORATE, ADMIN

        // Store ID: sadece CORPORATE kullanıcılar için, kendi store'u
        Long storeId = null;
        if (currentUser.getStore() != null) {
            storeId = currentUser.getStore().getId();
        }

        // Python agent'a gönderilecek payload
        Map<String, Object> payload = new HashMap<>();
        payload.put("question", userQuestion);
        payload.put("user_role", role);
        payload.put("user_id", userId);
        payload.put("store_id", storeId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(payload, headers);

        try {
            Map<String, Object> response = restTemplate.postForObject(agentUrl, entity, Map.class);
            if (response == null) return "Agent yanıt vermedi.";
            return (String) response.getOrDefault("answer", "Cevap alınamadı.");
        } catch (Exception e) {
            return "AI servisi şu anda kullanılamıyor: " + e.getMessage();
        }
    }
}
