package com.furkan.controllers.impl;

import com.furkan.controllers.IRestReviewController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoReviewRequest;
import com.furkan.dto.response.DtoReview;
import com.furkan.entities.User;
import com.furkan.services.IReviewService;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/reviews")
@RequiredArgsConstructor
public class RestReviewControllerImpl extends RestBaseController implements IRestReviewController {

    private final IReviewService reviewService;

    @PostMapping()
    @PreAuthorize("hasRole('INDIVIDUAL')")
    @Override
    public RootEntity<DtoReview> createReview(@Valid @RequestBody DtoReviewRequest request, @AuthenticationPrincipal UserDetails userDetails) {
        return ok(reviewService.createReview(request, getUserIdByToken(userDetails)));
    }

    @GetMapping("/product/{productId}")
    @Override
    public RootEntity<RestPageableEntity<DtoReview>> getProductReviews(@PathVariable Long productId, @ModelAttribute RestPageableRequest request) {
        return ok(reviewService.findProductReviews(productId, request));
    }

    @GetMapping()
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<RestPageableEntity<DtoReview>> findAllReviews(@ModelAttribute RestPageableRequest request) {
        return ok(reviewService.findAllReviews(request));
    }

    @GetMapping("/{id}")
    @Override
    public RootEntity<DtoReview> findReviewById(Long id) {
        return ok(reviewService.findReviewById(id));
    }

    @GetMapping("/{userId}/reviews")
    @Override
    public RootEntity<RestPageableEntity<DtoReview>> findReviewsByUser(@PathVariable Long userId, @ModelAttribute RestPageableRequest request) {
        return ok(reviewService.findReviewsByUser(userId, request));
    }

    @GetMapping("/my-reviews")
    @Override
    public RootEntity<RestPageableEntity<DtoReview>> findMyReviews(@AuthenticationPrincipal UserDetails userDetails, @ModelAttribute RestPageableRequest request) {
        return ok(reviewService.findReviewsByUser(getUserIdByToken(userDetails), request));
    }

    @GetMapping("/corporate/{storeId}/my-reviews")
    @Override
    public RootEntity<RestPageableEntity<DtoReview>> findReviewsByStoreId(@PathVariable Long storeId, @AuthenticationPrincipal UserDetails userDetails, @ModelAttribute RestPageableRequest request) {
        return ok(reviewService.findReviewByStoreId(storeId, getUserIdByToken(userDetails), request));
    }

    @PutMapping("/{reviewId}")
    @Override
    public RootEntity<DtoReview> updateReview(@PathVariable Long reviewId, @Valid @RequestBody DtoReviewRequest request, @AuthenticationPrincipal UserDetails userDetails) {
        return ok(reviewService.updateReview(reviewId, request, getUserIdByToken(userDetails)));
    }

    @DeleteMapping("/my-reviews/{reviewId}")
    @Override
    public RootEntity<Void> deleteMyReview(@PathVariable Long reviewId, @AuthenticationPrincipal UserDetails userDetails) {
        reviewService.deleteMyReview(reviewId, getUserIdByToken(userDetails));
        return ok();
    }

    @DeleteMapping("/{reviewId}")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<Void> deleteReview(Long reviewId) {
        reviewService.deleteReview(reviewId);
        return ok();
    }

    private Long getUserIdByToken(UserDetails userDetails) {
        return ((User) userDetails).getId();
    }
}
