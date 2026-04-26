package com.furkan.services.impl;

import com.furkan.dto.request.DtoAiRequest;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class AiServiceImpl implements IAiService {

    private final RestTemplate restTemplate;

    @Value("${ai.agent.url}")
    private String agentUrl;

    @Override
    public Map<String, Object> askAiIndividual(DtoAiRequest request, Authentication authentication) {
        User currentUser = (User) authentication.getPrincipal();
        return sendRequestToAgent(request.getQuestion(), RoleType.INDIVIDUAL.toString(), currentUser.getId(), null, request.getConversationHistory());
    }

    @Override
    public Map<String, Object> askAiCorporate(DtoAiRequest request, Authentication authentication) {
        User currentUser = (User) authentication.getPrincipal();
        if (currentUser.getStore() == null) {
            return Map.of("answer", "No store found associated with your account. Please contact support.");
        }
        Long storeId = currentUser.getStore().getId();
        return sendRequestToAgent(request.getQuestion(), RoleType.CORPORATE.toString(), currentUser.getId(), storeId, request.getConversationHistory());
    }

    @Override
    public Map<String, Object> askAiAdmin(DtoAiRequest request, Authentication authentication) {
        User currentUser = (User) authentication.getPrincipal();
        return sendRequestToAgent(request.getQuestion(), RoleType.ADMIN.toString(), currentUser.getId(), null, request.getConversationHistory());
    }

    private Map<String, Object> sendRequestToAgent(String question, String role, Long userId, Long storeId, List<Map<String, String>> conversationHistory) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("question", question);
        payload.put("user_role", role);
        payload.put("user_id", userId);
        payload.put("store_id", storeId);
        payload.put("conversation_history", conversationHistory != null ? conversationHistory : new ArrayList<>());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(payload, headers);

        try {
            Map<String, Object> response = restTemplate.postForObject(agentUrl, entity, Map.class);
            if (response == null) {
                return Map.of("answer", "Agent did not respond.");
            }
            return response;
        } catch (Exception e) {
            return Map.of("answer", "AI service is currently unavailable: " + e.getMessage());
        }
    }
}
