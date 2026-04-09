package com.furkan.services;

import com.furkan.dto.request.DtoCorporateUpdateRequest;
import com.furkan.dto.response.DtoCorporateUpdate;
import com.furkan.enums.CorporateUpgradeRequestStatus;

import java.util.List;

public interface ICorporateUpdateRequestService {

    DtoCorporateUpdate createRequest(DtoCorporateUpdateRequest request);

    DtoCorporateUpdate findRequestById(Long id);

    DtoCorporateUpdate findRequestByUserId(Long userId);

    DtoCorporateUpdate findRequestByUserEmail(String email);

    DtoCorporateUpdate approveRequest(DtoCorporateUpdateRequest request);

    DtoCorporateUpdate rejectRequest(DtoCorporateUpdateRequest request);

    List<DtoCorporateUpdate> findRequestsByStatus(CorporateUpgradeRequestStatus status);
}
