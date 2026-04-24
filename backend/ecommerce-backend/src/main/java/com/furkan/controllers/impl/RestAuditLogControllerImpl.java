package com.furkan.controllers.impl;

import com.furkan.controllers.IRestAuditLogController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.response.DtoAuditLog;
import com.furkan.services.IAuditLogService;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/api/admin")
@RequiredArgsConstructor
public class RestAuditLogControllerImpl extends RestBaseController implements IRestAuditLogController {

    private final IAuditLogService auditLogService;

    // eğer getmapping client tarafında sorun çıkartırsa post mapping e çek.
    @GetMapping("/audit-logs")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<RestPageableEntity<DtoAuditLog>> getAuditLogs(@ModelAttribute RestPageableRequest request) {
        return ok(auditLogService.getAuditLogs(request));
    }
}
