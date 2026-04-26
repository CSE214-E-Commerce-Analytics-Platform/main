package com.furkan.controllers.impl;

import com.furkan.controllers.IRestChatHistoryController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoChatHistoryRequest;
import com.furkan.dto.response.DtoChatHistory;
import com.furkan.entities.User;
import com.furkan.services.IChatHistoryService;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/chat-histories")
@RequiredArgsConstructor
public class RestChatHistoryControllerImpl extends RestBaseController implements IRestChatHistoryController {

    private final IChatHistoryService chatHistoryService;

    @PostMapping
    @Override
    public RootEntity<DtoChatHistory> create(
            @RequestBody DtoChatHistoryRequest request,
            @AuthenticationPrincipal UserDetails userDetails
            ) {
        return ok(chatHistoryService.create(request, getUserIdFromToken(userDetails)));
    }

    @GetMapping
    @Override
    public RootEntity<RestPageableEntity<DtoChatHistory>> getAll(@AuthenticationPrincipal UserDetails userDetails, RestPageableRequest request) {
        return ok(chatHistoryService.getAllByUser(getUserIdFromToken(userDetails), request));
    }

    @PatchMapping("/{id}/title")
    @Override
    public RootEntity<DtoChatHistory> updateTitle(
            @PathVariable Long id,
            @RequestBody Map<String, String> body,
            @AuthenticationPrincipal UserDetails userDetails
    ) {
        Long userId = getUserIdFromToken(userDetails);
        String newTitle = body.getOrDefault("title", "Untitled");
        return ok(chatHistoryService.updateTitle(id, newTitle, userId));
    }

    @DeleteMapping("/{id}")
    @Override
    public RootEntity<Void> delete(@PathVariable Long id, @AuthenticationPrincipal UserDetails userDetails) {
        chatHistoryService.delete(id, getUserIdFromToken(userDetails));
        return ok();
    }

    private Long getUserIdFromToken(UserDetails userDetails) {
        return ((User) userDetails).getId();
    }
}
