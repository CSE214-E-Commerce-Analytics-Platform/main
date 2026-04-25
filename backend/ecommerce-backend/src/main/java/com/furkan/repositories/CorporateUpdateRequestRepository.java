package com.furkan.repositories;

import com.furkan.entities.CorporateUpdateRequest;
import com.furkan.enums.CorporateUpdateRequestStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CorporateUpdateRequestRepository extends JpaRepository<CorporateUpdateRequest, Long> {

    Optional<CorporateUpdateRequest> findTopByUserIdOrderByCreatedAtDesc(Long userId);

    Page<CorporateUpdateRequest> findAllByStatus(CorporateUpdateRequestStatus status, Pageable pageable);

    boolean existsByUserIdAndStatus(Long userId, CorporateUpdateRequestStatus status);

    Optional<CorporateUpdateRequest> findByUserEmail(String email);
}
