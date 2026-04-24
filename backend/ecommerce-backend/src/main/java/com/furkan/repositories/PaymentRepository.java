package com.furkan.repositories;

import com.furkan.entities.Payment;
import com.furkan.enums.PaymentStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, Long> {

    Optional<Payment> findByOrderId(Long orderId);

    @Query("SELECT p FROM Payment p WHERE p.order.user.id = :userId")
    Page<Payment> findByUserId(@Param("userId") Long userId, Pageable pageable);

    @Query("SELECT p FROM Payment p WHERE p.order.id = :orderId AND p.status = :status")
    Optional<Payment> findByOrderIdAndStatus(@Param("orderId") Long orderId, @Param("status")PaymentStatus status);

    Optional<Payment> findByTransactionKey(String transactionKey);

    boolean existsByOrderId(Long orderId);
}
