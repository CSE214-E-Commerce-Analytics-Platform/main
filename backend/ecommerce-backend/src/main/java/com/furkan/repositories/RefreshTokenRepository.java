package com.furkan.repositories;

import com.furkan.entities.RefreshToken;
import com.furkan.entities.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

@Repository
public interface RefreshTokenRepository extends JpaRepository<RefreshToken, Long> {

    @Query("UPDATE RefreshToken r SET r.revokedAt = :now WHERE r.user = :user AND r.revokedAt IS NULL")
    @Modifying
    void revokeAllByUser(@Param("user") User user, @Param("now")LocalDateTime now);

    Optional<RefreshToken> findByToken(String token);

    @Modifying
    @Query("DELETE FROM RefreshToken r WHERE r.expiresAt < :now OR r.revokedAt IS NOT NULL")
    int deleteAllExpiredOrRevoked(@Param("now") LocalDateTime now);
}
