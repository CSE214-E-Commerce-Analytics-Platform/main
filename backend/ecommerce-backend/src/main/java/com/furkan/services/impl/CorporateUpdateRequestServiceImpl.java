package com.furkan.services.impl;

import com.furkan.dto.request.DtoCorporateCreateRequest;
import com.furkan.dto.request.DtoCorporateUpdateReviewRequest;
import com.furkan.dto.request.DtoStoreRequest;
import com.furkan.dto.response.DtoCorporateUpdate;
import com.furkan.dto.response.DtoStore;
import com.furkan.entities.CorporateUpdateRequest;
import com.furkan.entities.User;
import com.furkan.enums.CorporateUpdateRequestStatus;
import com.furkan.enums.RoleType;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.CartRepository;
import com.furkan.repositories.CorporateUpdateRequestRepository;
import com.furkan.repositories.UserRepository;
import com.furkan.services.ICorporateUpdateRequestService;
import com.furkan.services.IStoreService;
import com.furkan.utils.PagerUtil;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class CorporateUpdateRequestServiceImpl implements ICorporateUpdateRequestService {

    private final CorporateUpdateRequestRepository corporateUpdateRequestRepository;
    private final UserRepository userRepository;
    private final IStoreService storeService;
    private final CartRepository cartRepository;

    @Override
    @Transactional
    public DtoCorporateUpdate createRequest(Long userId, DtoCorporateCreateRequest request) {
        if (corporateUpdateRequestRepository.existsByUserIdAndStatus(userId, CorporateUpdateRequestStatus.PENDING)) {
            throw new BaseException(new ErrorMessage(MessageType.UPGRADE_ALREADY_REQUESTED, null));
        }

        User user = userRepository.findById(userId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, userId.toString())));

        CorporateUpdateRequest newRequest = new CorporateUpdateRequest();
        newRequest.setUser(user);
        newRequest.setCompanyName(request.getCompanyName());
        newRequest.setReason(request.getReason());
        newRequest.setStatus(CorporateUpdateRequestStatus.PENDING);

        CorporateUpdateRequest saved = corporateUpdateRequestRepository.save(newRequest);
        return mapToDto(saved);
    }

    @Override
    public DtoCorporateUpdate findMyLatestRequest(Long userId) {
        return corporateUpdateRequestRepository.findTopByUserIdOrderByCreatedAtDesc(userId)
                .map(this::mapToDto)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.UPGRADE_REQUEST_NOT_FOUND, userId.toString())));
    }

    @Override
    public DtoCorporateUpdate findRequestById(Long id) {
        return corporateUpdateRequestRepository.findById(id)
                .map(this::mapToDto)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.UPGRADE_REQUEST_NOT_FOUND_BY_ID, id.toString())));
    }

    @Override
    public DtoCorporateUpdate findRequestByUserEmail(String email) {
        return corporateUpdateRequestRepository.findByUserEmail(email)
                .map(this::mapToDto)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.UPGRADE_REQUEST_NOT_FOUND_BY_EMAIL, email)));
    }

    @Override
    @Transactional
    public DtoCorporateUpdate reviewRequest(Long id, DtoCorporateUpdateReviewRequest reviewDto) {
        CorporateUpdateRequest request = corporateUpdateRequestRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.UPGRADE_REQUEST_NOT_FOUND, id.toString())));

        if (request.getStatus() != CorporateUpdateRequestStatus.PENDING) {
            throw new BaseException(new ErrorMessage(MessageType.NOT_PENDING_REQUEST, null));
        }

        request.setStatus(reviewDto.getStatus());
        request.setAdminNote(reviewDto.getAdminNote());

        if (reviewDto.getStatus() == CorporateUpdateRequestStatus.APPROVED) {
            User user = request.getUser();
            user.setRoleType(RoleType.CORPORATE);
            userRepository.save(user);

            cartRepository.delete(user.getCart());

            DtoStoreRequest storeRequest = new DtoStoreRequest();
            storeRequest.setName(request.getCompanyName());

            DtoStore dtoStore = storeService.createStoreForCorporateUpgradeRole(user, storeRequest);
        }

        CorporateUpdateRequest updated = corporateUpdateRequestRepository.save(request);
        return mapToDto(updated);
    }

    @Override
    public RestPageableEntity<DtoCorporateUpdate> findRequestsByStatus(CorporateUpdateRequestStatus status, RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<CorporateUpdateRequest> corporateUpdateRequestPage = corporateUpdateRequestRepository.findAllByStatus(status, pageable);

        List<DtoCorporateUpdate> dtoList = corporateUpdateRequestPage.getContent().stream()
                .map(this::mapToDto)
                .toList();

        return PagerUtil.toPageableResponse(corporateUpdateRequestPage, dtoList);
    }

    private DtoCorporateUpdate mapToDto(CorporateUpdateRequest entity) {
        DtoCorporateUpdate dto = new DtoCorporateUpdate();
        BeanUtils.copyProperties(entity, dto);
        dto.setUserId(entity.getUser().getId());
        return dto;
    }
}
