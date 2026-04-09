package com.furkan.entities;

import com.furkan.enums.CorporateUpgradeRequestStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "corporate_update_requests")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CorporateUpdateRequest extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long Id;

    @Column(nullable = false)
    private String companyName;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String reason;

    @Enumerated(EnumType.STRING)
    private CorporateUpgradeRequestStatus status = CorporateUpgradeRequestStatus.PENDING;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String adminNote;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;
}
