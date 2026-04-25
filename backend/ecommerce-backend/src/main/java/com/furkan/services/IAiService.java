package com.furkan.services;

import org.springframework.security.core.Authentication;

import java.util.Map;

public interface IAiService {
    Map<String, Object> askAiIndividual(String userQuestion, Authentication authentication);

    Map<String, Object> askAiCorporate(String userQuestion, Authentication authentication);

    Map<String, Object> askAiAdmin(String userQuestion, Authentication authentication);
}
