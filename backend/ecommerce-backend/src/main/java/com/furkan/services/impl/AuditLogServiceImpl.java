package com.furkan.services.impl;

import com.furkan.dto.response.DtoAuditLog;
import com.furkan.entities.AuditLog;
import com.furkan.enums.RoleType;
import com.furkan.repositories.AuditLogRepository;
import com.furkan.services.IAuditLogService;
import com.furkan.utils.PagerUtil;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class AuditLogServiceImpl implements IAuditLogService {

    private final AuditLogRepository auditLogRepository;

    @Override
    public void logAction(Long userId, RoleType userRole, String action, String details) {
        AuditLog log = new AuditLog();
        log.setUserId(userId);
        log.setUserRole(userRole);
        log.setAction(action);
        log.setDetails(details);
        auditLogRepository.save(log);
    }

    @Override
    public RestPageableEntity<DtoAuditLog> getAuditLogs(RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("createdAt");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<AuditLog> logPage = auditLogRepository.findAll(pageable);

        List<DtoAuditLog> dtoList = logPage.getContent().stream()
                .map(this::dtoConverter)
                .collect(Collectors.toList());

        return PagerUtil.toPageableResponse(logPage, dtoList);
    }

    private DtoAuditLog dtoConverter(AuditLog auditLog) {
        DtoAuditLog dto = new DtoAuditLog();
        BeanUtils.copyProperties(auditLog, dto);
        if (auditLog.getUserRole() != null) {
            dto.setUserRole(auditLog.getUserRole().name());
        }
        return dto;
    }
}
