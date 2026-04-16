package com.furkan.services;

import org.springframework.security.core.Authentication;

public interface IAiService {
    String askAiIndividual(String userQuestion, Authentication authentication);

    String askAiCorporate(String userQuestion, Authentication authentication);

    String askAiAdmin(String userQuestion, Authentication authentication);
}
