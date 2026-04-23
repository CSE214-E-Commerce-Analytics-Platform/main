package com.furkan.repositories;

import com.furkan.entities.Shipment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface ShipmentRepository extends JpaRepository<Shipment, Long> {
    Optional<Shipment> findByTrackingNumber(String trackingNumber);
    Optional<Shipment> findByOrderId(Long orderId);

    @Query("SELECT s FROM Shipment s WHERE s.order.user.id = :userId")
    List<Shipment> findByUserId(Long userId);
}
