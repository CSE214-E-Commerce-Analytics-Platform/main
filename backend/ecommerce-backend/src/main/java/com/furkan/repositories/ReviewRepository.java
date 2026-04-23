package com.furkan.repositories;

import com.furkan.entities.Review;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ReviewRepository extends JpaRepository<Review, Long> {

    boolean existsByUserIdAndProductId(Long userId, Long productId);

    List<Review> findByProductIdOrderByCreatedAtDesc(Long productId);

    List<Review> findByUserIdOrderByCreatedAtDesc(Long userId);

    @Query("SELECT r FROM Review r JOIN r.product p WHERE p.store.id = :storeId")
    List<Review> findReviewByStoreId(@Param("storeId") Long storeId);
}
