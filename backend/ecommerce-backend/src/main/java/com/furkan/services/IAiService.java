package com.furkan.services;

import com.furkan.dto.request.DtoAiRequest;
import org.springframework.security.core.Authentication;

import java.util.Map;

public interface IAiService {
    Map<String, Object> askAiIndividual(DtoAiRequest request, Authentication authentication);

    Map<String, Object> askAiCorporate(DtoAiRequest request, Authentication authentication);

    Map<String, Object> askAiAdmin(DtoAiRequest request, Authentication authentication);
}
