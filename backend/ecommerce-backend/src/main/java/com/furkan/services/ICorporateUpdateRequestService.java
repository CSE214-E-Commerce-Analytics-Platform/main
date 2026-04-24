package com.furkan.services;

import com.furkan.dto.request.DtoCorporateCreateRequest;
import com.furkan.dto.request.DtoCorporateUpdateReviewRequest;
import com.furkan.dto.response.DtoCorporateUpdate;
import com.furkan.enums.CorporateUpdateRequestStatus;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;


public interface ICorporateUpdateRequestService {

    DtoCorporateUpdate createRequest(Long userId, DtoCorporateCreateRequest request);

    DtoCorporateUpdate findMyLatestRequest(Long userId);

    DtoCorporateUpdate findRequestById(Long id);

    DtoCorporateUpdate findRequestByUserEmail(String email);

    DtoCorporateUpdate reviewRequest(Long id, DtoCorporateUpdateReviewRequest reviewDto);

    RestPageableEntity<DtoCorporateUpdate> findRequestsByStatus(CorporateUpdateRequestStatus status, RestPageableRequest request);
}
