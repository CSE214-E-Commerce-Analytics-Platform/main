package com.furkan.controllers;

import com.furkan.dto.request.DtoCorporateCreateRequest;
import com.furkan.dto.request.DtoCorporateUpdateReviewRequest;
import com.furkan.dto.response.DtoCorporateUpdate;
import com.furkan.enums.CorporateUpdateRequestStatus;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

public interface IRestCorporateUpdateRequestController {

    RootEntity<DtoCorporateUpdate> createRequest(DtoCorporateCreateRequest request, UserDetails userDetails);

    RootEntity<DtoCorporateUpdate> findMyLatestRequest(UserDetails userDetails);

    RootEntity<DtoCorporateUpdate> findRequestById(Long id);

    RootEntity<DtoCorporateUpdate> findRequestByUserEmail(String email);

    RootEntity<DtoCorporateUpdate> reviewRequest(Long id, DtoCorporateUpdateReviewRequest reviewDto);

    RootEntity<List<DtoCorporateUpdate>> findRequestsByStatus(CorporateUpdateRequestStatus status);
}
