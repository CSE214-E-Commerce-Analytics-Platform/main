package com.furkan.services;

import com.furkan.dto.response.DtoAuditLog;
import com.furkan.enums.RoleType;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;

public interface IAuditLogService {

    void logAction(Long userId, RoleType userRole, String action, String details);

    RestPageableEntity<DtoAuditLog> getAuditLogs(RestPageableRequest request);
}
