package com.furkan.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DtoStoreRequest {
    @NotBlank(message = "Store name must not be empty.")
    private String name;

    @NotNull(message = "Store owner must be specified.")
    private Long ownerId;
}
