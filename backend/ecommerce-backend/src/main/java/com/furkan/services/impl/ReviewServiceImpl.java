package com.furkan.services.impl;

import com.furkan.dto.request.DtoReviewRequest;
import com.furkan.dto.response.DtoReview;
import com.furkan.entities.Product;
import com.furkan.entities.Review;
import com.furkan.entities.User;
import com.furkan.enums.OrderStatus;
import com.furkan.enums.Sentiment;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.*;
import com.furkan.services.IReviewService;
import com.furkan.utils.PagerUtil;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ReviewServiceImpl implements IReviewService {

    private final ReviewRepository reviewRepository;
    private final OrderRepository orderRepository;
    private final ProductRepository productRepository;
    private final UserRepository userRepository;

    @Override
    @Transactional
    public DtoReview createReview(DtoReviewRequest request, Long userId) {
        if (reviewRepository.existsByUserIdAndProductId(userId, request.getProductId())) {
            throw new BaseException(new ErrorMessage(MessageType.USER_ALREADY_REVIEWED_PRODUCT, request.getProductId().toString()));
        }

        boolean canReview = orderRepository.hasUserPurchasedAndReceivedProduct(
                userId,
                request.getProductId(),
                OrderStatus.DELIVERED
        );

        if (!canReview) {
            throw new BaseException(new ErrorMessage(MessageType.CAN_NOT_REVIEW_THIS_PRODUCT, request.getProductId().toString()));
        }

        User user = userRepository.findById(userId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, userId.toString())));

        Product product = productRepository.findById(request.getProductId())
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.PRODUCT_NOT_FOUND, request.getProductId().toString())));

        Review review = new Review();
        review.setUser(user);
        review.setProduct(product);
        review.setStarRating(request.getStarRating());
        review.setCommentText(request.getCommentText());
        review.setSentiment(calculateDefaultSentiment(request.getStarRating())); // Geçici yapay zeka simülasyonu
        review.setCreatedAt(LocalDateTime.now());
        review.setUpdatedAt(LocalDateTime.now());

        Review savedReview = reviewRepository.save(review);
        return dtoConverter(savedReview);
    }

    @Override
    public RestPageableEntity<DtoReview> findProductReviews(Long productId, RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Review> reviewPage = reviewRepository.findByProductIdOrderByCreatedAtDesc(productId, pageable);

        List<DtoReview> dtoList = reviewPage.getContent().stream()
                .map(this::dtoConverter)
                .toList();

        return PagerUtil.toPageableResponse(reviewPage, dtoList);
    }

    @Override
    public RestPageableEntity<DtoReview> findAllReviews(RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Review> reviewPage = reviewRepository.findAll(pageable);

        List<DtoReview> dtoList = reviewPage.getContent().stream()
                .map(this::dtoConverter)
                .toList();

        return PagerUtil.toPageableResponse(reviewPage, dtoList);
    }

    @Override
    public DtoReview findReviewById(Long id) {
        Review review = reviewRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.REVIEW_NOT_FOUND, id.toString())));
        return dtoConverter(review);
    }

    @Override
    public RestPageableEntity<DtoReview> findReviewsByUser(Long userId, RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Review> reviewPage = reviewRepository.findByUserIdOrderByCreatedAtDesc(userId, pageable);

        List<DtoReview> dtoList = reviewPage.getContent().stream()
                .map(this::dtoConverter)
                .toList();

        return PagerUtil.toPageableResponse(reviewPage, dtoList);
    }

    @Override
    public RestPageableEntity<DtoReview> findReviewByStoreId(Long storeId, Long userId, RestPageableRequest request) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, userId.toString())));

        if (!user.getStore().getId().equals(storeId)) {
            throw new BaseException(new ErrorMessage(MessageType.UNAUTHORIZED, "You can not access other corporate persons reviews."));
        }

        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Review> reviewPage = reviewRepository.findReviewByStoreId(storeId, pageable);

        List<DtoReview> dtoList = reviewPage.getContent().stream()
                .map(this::dtoConverter)
                .toList();

        return PagerUtil.toPageableResponse(reviewPage, dtoList);
    }

    @Override
    @Transactional
    public DtoReview updateReview(Long reviewId, DtoReviewRequest request, Long userId) {
        Review review = reviewRepository.findById(reviewId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.REVIEW_NOT_FOUND, reviewId.toString())));

        review.setCommentText(request.getCommentText());
        review.setStarRating(request.getStarRating());

        review.setSentiment(calculateDefaultSentiment(request.getStarRating()));
        return dtoConverter(reviewRepository.save(review));
    }

    @Override
    @Transactional
    public void deleteReview(Long reviewId) {
        Review review = reviewRepository.findById(reviewId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.REVIEW_NOT_FOUND, reviewId.toString())));

        reviewRepository.delete(review);
    }

    @Override
    @Transactional
    public void deleteMyReview(Long reviewId, Long userId) {
        Review review = reviewRepository.findById(reviewId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.REVIEW_NOT_FOUND, reviewId.toString())));

        if (!review.getUser().getId().equals(userId)) {
            throw new BaseException(new ErrorMessage(MessageType.REVIEW_OWNER_IS_DIFFERENT, reviewId.toString()));
        }

        reviewRepository.delete(review);
    }

    // Yıldız sayısına göre basit duygu analizi kuralı
    private Sentiment calculateDefaultSentiment(Integer starRating) {
        if (starRating >= 4) return Sentiment.POSITIVE;
        if (starRating == 3) return Sentiment.NEUTRAL;
        return Sentiment.NEGATIVE;
    }

    private DtoReview dtoConverter(Review review) {
        DtoReview dto = new DtoReview();
        BeanUtils.copyProperties(review, dto);
        dto.setProductId(review.getProduct().getId());
        dto.setUserId(review.getUser().getId());
        return dto;
    }
}
