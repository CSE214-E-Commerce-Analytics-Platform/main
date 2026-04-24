package com.furkan.controllers;

import com.furkan.dto.response.DtoAuditLog;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;

public interface IRestAuditLogController {

    RootEntity<RestPageableEntity<DtoAuditLog>> getAuditLogs(RestPageableRequest request);
}
