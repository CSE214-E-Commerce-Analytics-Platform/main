package com.furkan.controllers;

import com.furkan.dto.request.DtoReviewRequest;
import com.furkan.dto.response.DtoReview;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

public interface IRestReviewController {

    RootEntity<DtoReview> createReview(DtoReviewRequest request, UserDetails userDetails);

    RootEntity<List<DtoReview>> getProductReviews(Long productId);

    RootEntity<List<DtoReview>> findAllReviews();

    RootEntity<DtoReview> findReviewById(Long id);

    RootEntity<List<DtoReview>> findReviewsByUser(Long userId);

    RootEntity<List<DtoReview>> findMyReviews(UserDetails userDetails);

    RootEntity<List<DtoReview>> findReviewsByStoreId(Long storeId, UserDetails userDetails);

    RootEntity<DtoReview> updateReview(Long reviewId, DtoReviewRequest request, UserDetails userDetails);

    RootEntity<Void> deleteMyReview(Long reviewId, UserDetails userDetails);

    RootEntity<Void> deleteReview(Long reviewId);
}
