package com.furkan.services;

import com.furkan.dto.request.DtoReviewRequest;
import com.furkan.dto.response.DtoReview;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;

public interface IReviewService {

    DtoReview createReview(DtoReviewRequest request, Long userId);

    RestPageableEntity<DtoReview> findProductReviews(Long productId, RestPageableRequest request);

    RestPageableEntity<DtoReview> findAllReviews(RestPageableRequest request);

    DtoReview findReviewById(Long id);

    RestPageableEntity<DtoReview> findReviewsByUser(Long userId, RestPageableRequest request);

    RestPageableEntity<DtoReview> findReviewByStoreId(Long storeId, Long userId, RestPageableRequest request);

    DtoReview updateReview(Long reviewId, DtoReviewRequest request, Long userId);

    void deleteReview(Long reviewId);

    void deleteMyReview(Long reviewId, Long userId);
}
