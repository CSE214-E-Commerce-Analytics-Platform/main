package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoAuditLog extends BaseDto {
    private Long userId;
    private String userRole;
    private String action;
    private String details;
}
