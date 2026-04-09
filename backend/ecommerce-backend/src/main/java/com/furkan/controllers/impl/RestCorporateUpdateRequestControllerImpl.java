package com.furkan.controllers.impl;

import com.furkan.controllers.IRestCorporateUpdateRequestController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoCorporateCreateRequest;
import com.furkan.dto.request.DtoCorporateUpdateReviewRequest;
import com.furkan.dto.response.DtoCorporateUpdate;
import com.furkan.entities.User;
import com.furkan.enums.CorporateUpdateRequestStatus;
import com.furkan.services.ICorporateUpdateRequestService;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/corporate-applications")
@RequiredArgsConstructor
public class RestCorporateUpdateRequestControllerImpl extends RestBaseController implements IRestCorporateUpdateRequestController {

    private final ICorporateUpdateRequestService service;

    @PostMapping()
    @PreAuthorize("hasRole('INDIVIDUAL')")
    @Override
    public RootEntity<DtoCorporateUpdate> createRequest(@RequestBody DtoCorporateCreateRequest request, @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = ((User) userDetails).getId();
        return ok(service.createRequest(userId, request));
    }

    @GetMapping("/my-request")
    @Override
    public RootEntity<DtoCorporateUpdate> findMyLatestRequest(@AuthenticationPrincipal UserDetails userDetails) {
        Long userId = ((User) userDetails).getId();
        return ok(service.findMyLatestRequest(userId));
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<DtoCorporateUpdate> findRequestById(@PathVariable Long id) {
        return ok(service.findRequestById(id));
    }

    @GetMapping("/email")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<DtoCorporateUpdate> findRequestByUserEmail(@RequestParam String email) {
        return ok(service.findRequestByUserEmail(email));
    }

    @PutMapping("/{id}/review")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<DtoCorporateUpdate> reviewRequest(@PathVariable Long id, @RequestBody DtoCorporateUpdateReviewRequest reviewDto) {
        return ok(service.reviewRequest(id, reviewDto));
    }

    @GetMapping("/all-by-status")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<List<DtoCorporateUpdate>> findRequestsByStatus(@RequestParam CorporateUpdateRequestStatus status) {
        return ok(service.findRequestsByStatus(status));
    }
}
