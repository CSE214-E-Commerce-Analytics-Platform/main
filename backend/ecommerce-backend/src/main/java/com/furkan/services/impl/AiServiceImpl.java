package com.furkan.services.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.furkan.dto.response.DtoProduct;
import com.furkan.services.IAiService;
import com.furkan.services.IProductService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class AiServiceImpl implements IAiService {

    private final IProductService productService;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    @Value("${google.gemini.api.key}")
    private String apiKey;

    private static final String GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent?key=";

    @Override
    public String askAi(String userQuestion, Long storeId) {
        // 1. Veriyi çek
        List<DtoProduct> authorizedProducts = productService.findAllByStoreId(storeId);

        // 2. JSON'a çevir (AI'ın okuyabilmesi için kritik)
        String productsJson;
        try {
            productsJson = objectMapper.writeValueAsString(authorizedProducts);
        } catch (JsonProcessingException e) {
            productsJson = "[]";
        }

        // 3. System Instruction'ı daha esnek ve profesyonel hale getir
        String systemInstruction = String.format(
                "You are a helpful E-Commerce Analytics Assistant. " +
                        "You have access to the following product list in JSON format: %s " +
                        "\nRULES:" +
                        "\n1. Answer ONLY based on the provided JSON data." +
                        "\n2. If the user asks about a product not in the JSON, politely state it is unavailable." +
                        "\n3. Be professional and concise. Answer in the language of the user's question.",
                productsJson
        );

        // Request Body hazırlama (Mevcut mantığın doğru, devam edebilirsin)
        Map<String, Object> requestBody = Map.of(
                "contents", List.of(
                        Map.of("parts", List.of(
                                Map.of("text", systemInstruction + "\n\nUser Question: " + userQuestion)
                        ))
                )
        );


        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestBody, headers);

            String finalUrl = GEMINI_URL + apiKey;
            Map<String, Object> response = restTemplate.postForObject(finalUrl, entity, Map.class);

            return parseGeminiResponse(response);
        } catch (Exception e) {
            return "Error: AI service is currently unavailable. " + e.getMessage();
        }
    }

    private String parseGeminiResponse(Map<String, Object> response) {
        try {
            List candidates = (List) response.get("candidates");
            if (candidates == null || candidates.isEmpty()) return "No candidates found.";

            Map firstCandidate = (Map) candidates.get(0);
            Map contentMap = (Map) firstCandidate.get("content");

            List parts = (List) contentMap.get("parts");
            if (parts == null || parts.isEmpty()) return "No parts found.";

            Map firstPart = (Map) parts.get(0);
            return firstPart.get("text").toString();
        } catch (Exception e) {
            return "Error parsing AI response: " + e.getMessage();
        }
    }
}
