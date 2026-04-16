package com.furkan.controllers;

import com.furkan.dto.request.DtoAiRequest;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.Authentication;

public interface IRestAiController {

    RootEntity<String> askIndividualAi(DtoAiRequest request, Authentication authentication);
    RootEntity<String> askCorporateAi(DtoAiRequest request, Authentication authentication);
    RootEntity<String> askAdminAi(DtoAiRequest request, Authentication authentication);
}
