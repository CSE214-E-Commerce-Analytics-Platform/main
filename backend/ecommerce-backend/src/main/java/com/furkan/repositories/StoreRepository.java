package com.furkan.repositories;

import com.furkan.entities.Store;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface StoreRepository extends JpaRepository<Store, Long> {

    Optional<Store> findByIdAndOwnerId(Long id, Long ownerId);

    boolean existsByOwnerId(Long ownerId);

    Page<Store> findAllByOwnerId(Long ownerId, Pageable pageable);
}
