package com.furkan.dto.request;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DtoReviewRequest {
    @NotNull(message = "Product ID must not be null")
    private Long productId;

    @NotNull(message = "Star rating must not be null")
    @Min(value = 1, message = "The score must be at least 1.")
    @Max(value = 5, message = "The score must be at most 5.")
    private Integer starRating;

    private String commentText;
}
