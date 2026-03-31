package com.furkan.dto.request;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class DtoStoreRequest {
    @NotBlank(message = "Store name must not be empty.")
    private String name;
}
