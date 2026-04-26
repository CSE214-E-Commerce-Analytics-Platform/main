package com.furkan.controllers;

import com.furkan.dto.request.DtoChatHistoryRequest;
import com.furkan.dto.response.DtoChatHistory;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.Map;

public interface IRestChatHistoryController {

    RootEntity<DtoChatHistory> create(DtoChatHistoryRequest request, UserDetails userDetails);

    RootEntity<RestPageableEntity<DtoChatHistory>> getAll(UserDetails userDetails, RestPageableRequest request);

    RootEntity<DtoChatHistory> updateTitle(Long id, Map<String, String> body, UserDetails userDetails);

    RootEntity<Void> delete(Long id, UserDetails userDetails);
}
