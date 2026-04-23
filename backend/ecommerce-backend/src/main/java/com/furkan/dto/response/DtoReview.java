package com.furkan.dto.response;

import com.furkan.dto.BaseDto;
import com.furkan.enums.Sentiment;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DtoReview extends BaseDto {
    private Long productId;
    private Long userId;
    private Integer starRating;
    private String commentText;
    private Sentiment sentiment;
}
