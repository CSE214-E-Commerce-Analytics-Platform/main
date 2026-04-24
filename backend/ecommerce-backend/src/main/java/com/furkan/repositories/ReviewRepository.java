package com.furkan.repositories;

import com.furkan.entities.Review;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ReviewRepository extends JpaRepository<Review, Long> {

    boolean existsByUserIdAndProductId(Long userId, Long productId);

    Page<Review> findByProductIdOrderByCreatedAtDesc(Long productId, Pageable pageable);

    Page<Review> findByUserIdOrderByCreatedAtDesc(Long userId, Pageable pageable);

    @Query("SELECT r FROM Review r JOIN r.product p WHERE p.store.id = :storeId")
    Page<Review> findReviewByStoreId(@Param("storeId") Long storeId, Pageable pageable);
}
