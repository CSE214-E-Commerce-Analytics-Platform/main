package com.furkan.controllers.impl;

import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.ChatHistoryRequest;
import com.furkan.dto.response.ChatHistoryResponse;
import com.furkan.entities.User;
import com.furkan.services.ChatHistoryService;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/history")
@RequiredArgsConstructor
public class RestChatHistoryControllerImpl extends RestBaseController {

    private final ChatHistoryService chatHistoryService;

    @PostMapping
    public RootEntity<ChatHistoryResponse> create(
            @RequestBody ChatHistoryRequest request,
            Authentication authentication
    ) {
        User user = (User) authentication.getPrincipal();
        return ok(chatHistoryService.create(request, user.getId()));
    }

    @GetMapping
    public RootEntity<List<ChatHistoryResponse>> getAll(Authentication authentication) {
        User user = (User) authentication.getPrincipal();
        return ok(chatHistoryService.getAllByUser(user.getId()));
    }

    @PatchMapping("/{id}/title")
    public RootEntity<ChatHistoryResponse> updateTitle(
            @PathVariable UUID id,
            @RequestBody Map<String, String> body,
            Authentication authentication
    ) {
        User user = (User) authentication.getPrincipal();
        String newTitle = body.getOrDefault("title", "Untitled");
        return ok(chatHistoryService.updateTitle(id, newTitle, user.getId()));
    }

    @DeleteMapping("/{id}")
    public RootEntity<Void> delete(@PathVariable UUID id, Authentication authentication) {
        User user = (User) authentication.getPrincipal();
        chatHistoryService.delete(id, user.getId());
        return ok();
    }
}
