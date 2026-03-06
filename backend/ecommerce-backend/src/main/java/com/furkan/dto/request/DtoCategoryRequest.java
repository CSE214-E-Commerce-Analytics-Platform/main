package com.furkan.dto.request;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class DtoCategoryRequest {
    @NotBlank(message = "Category name must not be empty.")
    private String name;

    private Long parentId;
}
