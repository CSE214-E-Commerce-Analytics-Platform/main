package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import com.furkan.enums.CorporateUpgradeRequestStatus;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoCorporateUpdate extends BaseDto {

    private Long userId;
    private String companyName;
    private String reason;
    private CorporateUpgradeRequestStatus status;
    private String adminNote;
}
