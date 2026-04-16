package com.furkan.services.impl;

import com.furkan.entities.User;
import com.furkan.enums.RoleType;
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
    public String askAiIndividual(String userQuestion, Authentication authentication) {
        User currentUser = (User) authentication.getPrincipal();
        // Individual users do not have a storeId, so we send null.
        return sendRequestToAgent(userQuestion, RoleType.INDIVIDUAL.toString(), currentUser.getId(), null);
    }

    @Override
    public String askAiCorporate(String userQuestion, Authentication authentication) {
        User currentUser = (User) authentication.getPrincipal();

        // If the corporate user has no store assigned, block the request directly without hitting the AI service.
        if (currentUser.getStore() == null) {
            return "No store found associated with your account. Please contact support.";
        }

        Long storeId = currentUser.getStore().getId();
        return sendRequestToAgent(userQuestion, RoleType.CORPORATE.toString(), currentUser.getId(), storeId);
    }

    @Override
    public String askAiAdmin(String userQuestion, Authentication authentication) {
        User currentUser = (User) authentication.getPrincipal();
        // Admins can access all data, so no storeId restriction is sent.
        return sendRequestToAgent(userQuestion, RoleType.ADMIN.toString(), currentUser.getId(), null);
    }

    private String sendRequestToAgent(String question, String role, Long userId, Long storeId) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("question", question);
        payload.put("user_role", role);
        payload.put("user_id", userId);
        payload.put("store_id", storeId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(payload, headers);

        try {
            Map<String, Object> response = restTemplate.postForObject(agentUrl, entity, Map.class);
            if (response == null) {
                return "Agent did not respond.";
            }
            return (String) response.getOrDefault("answer", "Could not get a response.");
        } catch (Exception e) {
            return "AI service is currently unavailable: " + e.getMessage();
        }
    }
}
