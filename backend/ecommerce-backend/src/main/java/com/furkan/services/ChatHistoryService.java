package com.furkan.services;

import com.furkan.dto.request.ChatHistoryRequest;
import com.furkan.dto.response.ChatHistoryResponse;

import java.util.List;
import java.util.UUID;

public interface ChatHistoryService {
    ChatHistoryResponse create(ChatHistoryRequest request, Long userId);
    List<ChatHistoryResponse> getAllByUser(Long userId);
    ChatHistoryResponse updateTitle(UUID id, String newTitle, Long userId);
    void delete(UUID id, Long userId);
}
