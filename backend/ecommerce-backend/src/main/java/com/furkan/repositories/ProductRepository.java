package com.furkan.repositories;

import com.furkan.entities.Product;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {

    Page<Product> findAllByStoreId(Long storeId, Pageable pageable);

    boolean existsBySku(String sku);

    @Query("SELECT p FROM Product p WHERE p.id = :productId AND p.store.owner.id = :ownerId")
    Optional<Product> findByIdAndStoreOwnerId(@Param("productId") Long productId, @Param("ownerId") Long ownerId);
}
