package com.furkan.entities;

import com.furkan.enums.RoleType;
import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "users")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class User extends BaseEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String email;

    @Column(nullable = false)
    private String passwordHash;

    @Enumerated(EnumType.STRING)
    private RoleType roleType = RoleType.INDIVIDUAL;

    private String gender;

    private boolean isActive = true;

    @OneToOne(mappedBy = "owner")
    private Store store;
}
