package com.furkan.repositories;

import com.furkan.entities.Order;
import com.furkan.enums.OrderStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {

    Page<Order> findByUserIdAndParentOrderIsNullOrderByOrderDateDesc(Long userId, Pageable pageable);

    Page<Order> findByStoreIdOrderByOrderDateDesc(Long storeId, Pageable pageable);

    // For Analytics
    List<Order> findAllByStoreId(Long storeId);

    List<Order> findByParentOrderId(Long parentOrderId);

    @Query("SELECT COUNT(o) > 0 FROM Order o JOIN o.orderItems oi WHERE o.user.id = :userId AND oi.product.id = :productId AND o.status = :status")
    boolean hasUserPurchasedAndReceivedProduct(@Param("userId") Long userId, @Param("productId") Long productId, @Param("status") OrderStatus orderStatus);
}
