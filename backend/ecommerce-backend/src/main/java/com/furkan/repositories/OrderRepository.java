package com.furkan.repositories;

import com.furkan.entities.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {

    List<Order> findByUserIdAndParentOrderIsNullOrderByOrderDateDesc(Long userId);

    List<Order> findByStoreIdOrderByOrderDateDesc(Long storeId);

    List<Order> findByParentOrderId(Long parentOrderId);
}
