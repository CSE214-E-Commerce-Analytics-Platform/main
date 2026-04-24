package com.furkan.controllers;

import com.furkan.dto.request.DtoReviewRequest;
import com.furkan.dto.response.DtoReview;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

public interface IRestReviewController {

    RootEntity<DtoReview> createReview(DtoReviewRequest request, UserDetails userDetails);

    RootEntity<RestPageableEntity<DtoReview>> getProductReviews(Long productId, RestPageableRequest request);

    RootEntity<RestPageableEntity<DtoReview>> findAllReviews(RestPageableRequest request);

    RootEntity<DtoReview> findReviewById(Long id);

    RootEntity<RestPageableEntity<DtoReview>> findReviewsByUser(Long userId, RestPageableRequest request);

    RootEntity<RestPageableEntity<DtoReview>> findMyReviews(UserDetails userDetails, RestPageableRequest request);

    RootEntity<RestPageableEntity<DtoReview>> findReviewsByStoreId(Long storeId, UserDetails userDetails, RestPageableRequest request);

    RootEntity<DtoReview> updateReview(Long reviewId, DtoReviewRequest request, UserDetails userDetails);

    RootEntity<Void> deleteMyReview(Long reviewId, UserDetails userDetails);

    RootEntity<Void> deleteReview(Long reviewId);
}
