package com.furkan.dto.request;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class DtoCorporateCreateRequest {
    @NotBlank(message = "Reason must be not empty!")
    private String reason;

    @NotBlank(message = "Company name must be not empty!")
    private String companyName;
}
