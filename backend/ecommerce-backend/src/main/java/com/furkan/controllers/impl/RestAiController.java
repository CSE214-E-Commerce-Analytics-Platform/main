package com.furkan.controllers.impl;

import com.furkan.controllers.IRestAiController;
import com.furkan.controllers.RestBaseController;
import com.furkan.services.IAiService;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/ai")
@RequiredArgsConstructor
public class RestAiController extends RestBaseController implements IRestAiController {

    private final IAiService aiService;

    @PostMapping("/ask")
    @Override
    public RootEntity<String> askAi(@RequestParam String question, @RequestParam Long storeId) {
        return ok(aiService.askAi(question, storeId));
    }
}
