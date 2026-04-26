package com.furkan.services.impl;

import com.furkan.dto.request.DtoChatHistoryRequest;
import com.furkan.dto.response.DtoChatHistory;
import com.furkan.entities.ChatHistory;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.ChatHistoryRepository;
import com.furkan.services.IChatHistoryService;
import com.furkan.utils.PagerUtil;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ChatHistoryServiceImpl implements IChatHistoryService {

    private final ChatHistoryRepository repository;

    @Override
    @Transactional
    public DtoChatHistory create(DtoChatHistoryRequest request, Long userId) {
        ChatHistory entity = new ChatHistory();
        entity.setTitle(request.getTitle());
        entity.setInitialQuery(request.getInitialQuery());
        entity.setUserId(userId);
        ChatHistory saved = repository.save(entity);
        return toResponse(saved);
    }

    @Override
    public RestPageableEntity<DtoChatHistory> getAllByUser(Long userId, RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<ChatHistory> chatHistoryPage = repository.findByUserIdOrderByCreatedAtDesc(userId, pageable);

        List<DtoChatHistory> dtoList = chatHistoryPage.getContent().stream()
                .map(this::toResponse)
                .toList();

        return PagerUtil.toPageableResponse(chatHistoryPage, dtoList);
    }

    @Override
    @Transactional
    public DtoChatHistory updateTitle(Long id, String newTitle, Long userId) {
        ChatHistory entity = repository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.CHAT_NOT_FOUND, id.toString())));
        if (!entity.getUserId().equals(userId)) {
            throw new BaseException(new ErrorMessage(MessageType.UNAUTHORIZED, userId.toString()));
        }
        entity.setTitle(newTitle);
        ChatHistory updated = repository.save(entity);
        return toResponse(updated);
    }

    @Override
    @Transactional
    public void delete(Long id, Long userId) {
        ChatHistory entity = repository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.CHAT_NOT_FOUND, id.toString())));
        if (!entity.getUserId().equals(userId)) {
            throw new BaseException(new ErrorMessage(MessageType.UNAUTHORIZED, userId.toString()));
        }
        repository.delete(entity);
    }

    private DtoChatHistory toResponse(ChatHistory entity) {
        DtoChatHistory dto = new DtoChatHistory();
        BeanUtils.copyProperties(entity, dto);
        if (entity.getUserId() != null) {
            dto.setUserId(entity.getUserId());
        }
        return dto;
    }
}
