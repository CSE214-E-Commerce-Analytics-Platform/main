package com.furkan.controllers;

import com.furkan.dto.request.DtoAiRequest;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.Authentication;

import java.util.Map;

public interface IRestAiController {

    RootEntity<Map<String, Object>> askIndividualAi(DtoAiRequest request, Authentication authentication);
    RootEntity<Map<String, Object>> askCorporateAi(DtoAiRequest request, Authentication authentication);
    RootEntity<Map<String, Object>> askAdminAi(DtoAiRequest request, Authentication authentication);
}
