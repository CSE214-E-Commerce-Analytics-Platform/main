package com.furkan.repositories;

import com.furkan.entities.CorporateUpdateRequest;
import com.furkan.enums.CorporateUpgradeRequestStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CorporateUpdateRequestRepository extends JpaRepository<CorporateUpdateRequest, Long> {

    Optional<CorporateUpdateRequest> findByUserId(Long userId);

    List<CorporateUpdateRequest> findByStatus(CorporateUpgradeRequestStatus status);
}
