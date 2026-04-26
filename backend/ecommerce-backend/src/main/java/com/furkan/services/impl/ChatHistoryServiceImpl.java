package com.furkan.services.impl;

import com.furkan.dto.request.ChatHistoryRequest;
import com.furkan.dto.response.ChatHistoryResponse;
import com.furkan.entities.ChatHistory;
import com.furkan.repositories.ChatHistoryRepository;
import com.furkan.services.ChatHistoryService;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ChatHistoryServiceImpl implements ChatHistoryService {

    private final ChatHistoryRepository repository;

    @Override
    @Transactional
    public ChatHistoryResponse create(ChatHistoryRequest request, Long userId) {
        ChatHistory entity = new ChatHistory();
        entity.setTitle(request.getTitle());
        entity.setInitialQuery(request.getInitialQuery());
        entity.setUserId(userId);
        ChatHistory saved = repository.save(entity);
        return toResponse(saved);
    }

    @Override
    @Transactional(readOnly = true)
    public List<ChatHistoryResponse> getAllByUser(Long userId) {
        return repository.findByUserIdOrderByCreatedAtDesc(userId)
                .stream()
                .map(this::toResponse)
                .collect(Collectors.toList());
    }

    @Override
    @Transactional
    public ChatHistoryResponse updateTitle(UUID id, String newTitle, Long userId) {
        ChatHistory entity = repository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("Chat history not found: " + id));
        if (!entity.getUserId().equals(userId)) {
            throw new SecurityException("Access denied to chat history: " + id);
        }
        entity.setTitle(newTitle);
        ChatHistory updated = repository.save(entity);
        return toResponse(updated);
    }

    @Override
    @Transactional
    public void delete(UUID id, Long userId) {
        ChatHistory entity = repository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("Chat history not found: " + id));
        if (!entity.getUserId().equals(userId)) {
            throw new SecurityException("Access denied to chat history: " + id);
        }
        repository.delete(entity);
    }

    private ChatHistoryResponse toResponse(ChatHistory entity) {
        return ChatHistoryResponse.builder()
                .id(entity.getId())
                .title(entity.getTitle())
                .initialQuery(entity.getInitialQuery())
                .userId(entity.getUserId())
                .createdAt(entity.getCreatedAt())
                .updatedAt(entity.getUpdatedAt())
                .build();
    }
}
