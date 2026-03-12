package com.furkan.repositories;

import com.furkan.entities.User;
import com.furkan.entities.VerificationToken;
import com.furkan.enums.TokenType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

@Repository
public interface VerificationTokenRepository extends JpaRepository<VerificationToken, Long> {

    Optional<VerificationToken> findByTokenAndTokenType(String token, TokenType tokenType);

    @Modifying
    @Query("UPDATE VerificationToken v SET v.usedAt = :now " +
            "WHERE v.user =:user AND v.tokenType = :tokenType AND v.usedAt IS NULL")
    void invalidateAllByUserAndTokenType(@Param("user")User user,
                                         @Param("tokenType") TokenType tokenType,
                                         @Param("now")LocalDateTime now);
}
