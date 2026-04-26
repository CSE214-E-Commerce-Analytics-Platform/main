package com.furkan.repositories;

import com.furkan.entities.Shipment;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface ShipmentRepository extends JpaRepository<Shipment, Long> {
    Optional<Shipment> findByTrackingNumber(String trackingNumber);
    Optional<Shipment> findByOrderId(Long orderId);

    @Query("SELECT s FROM Shipment s WHERE s.order.user.id = :userId")
    Page<Shipment> findByUserId(Long userId, Pageable pageable);

    @Query("SELECT s FROM Shipment s WHERE s.order.store.owner.id = :ownerId")
    Page<Shipment> findByStoreOwnerId(Long ownerId, Pageable pageable);

    @Query("SELECT s FROM Shipment s WHERE s.order.id = :orderId")
    List<Shipment> findAllByOrderId(Long orderId);
}
