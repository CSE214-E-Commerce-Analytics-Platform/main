package com.furkan.dto.request;


import com.furkan.enums.CorporateUpdateRequestStatus;
import lombok.Data;

@Data
public class DtoCorporateUpdateReviewRequest {
    private CorporateUpdateRequestStatus status;
    private String adminNote;
}
