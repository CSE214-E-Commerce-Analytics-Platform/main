package com.furkan.services;

import com.furkan.dto.request.DtoChatHistoryRequest;
import com.furkan.dto.response.DtoChatHistory;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;

public interface IChatHistoryService {
    DtoChatHistory create(DtoChatHistoryRequest request, Long userId);
    RestPageableEntity<DtoChatHistory> getAllByUser(Long userId, RestPageableRequest request);
    DtoChatHistory updateTitle(Long id, String newTitle, Long userId);
    void delete(Long id, Long userId);
}
