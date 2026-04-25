package com.furkan.controllers.impl;

import com.furkan.controllers.IRestAiController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoAiRequest;
import com.furkan.services.IAiService;
import com.furkan.utils.RootEntity;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
@RequestMapping("/api/ai")
@RequiredArgsConstructor
public class RestAiControllerImpl extends RestBaseController implements IRestAiController {

    private final IAiService aiService;

    @PostMapping("/ask/individual")
    @PreAuthorize("hasRole('INDIVIDUAL')")
    @Override
    public RootEntity<Map<String, Object>> askIndividualAi(@Valid @RequestBody DtoAiRequest request, Authentication authentication) {
        return ok(aiService.askAiIndividual(request.getQuestion(), authentication));
    }

    @PostMapping("/ask/corporate")
    @PreAuthorize("hasRole('CORPORATE')")
    @Override
    public RootEntity<Map<String, Object>> askCorporateAi(@Valid @RequestBody DtoAiRequest request, Authentication authentication) {
        return ok(aiService.askAiCorporate(request.getQuestion(), authentication));
    }

    @PostMapping("/ask/admin")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<Map<String, Object>> askAdminAi(@RequestBody DtoAiRequest request, Authentication authentication) {
        return ok(aiService.askAiAdmin(request.getQuestion(), authentication));
    }
}
