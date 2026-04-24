package com.furkan.services;

import com.furkan.dto.request.DtoReviewRequest;
import com.furkan.dto.response.DtoReview;

import java.util.List;

public interface IReviewService {

    DtoReview createReview(DtoReviewRequest request, Long userId);

    List<DtoReview> findProductReviews(Long productId);

    List<DtoReview> findAllReviews();

    DtoReview findReviewById(Long id);

    List<DtoReview> findReviewsByUser(Long userId);

    List<DtoReview> findReviewByStoreId(Long storeId, Long userId);

    DtoReview updateReview(Long reviewId, DtoReviewRequest request, Long userId);

    void deleteReview(Long reviewId);

    void deleteMyReview(Long reviewId, Long userId);
}
