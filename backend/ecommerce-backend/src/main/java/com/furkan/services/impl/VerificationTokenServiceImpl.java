package com.furkan.services.impl;

import com.furkan.entities.User;
import com.furkan.entities.VerificationToken;
import com.furkan.enums.TokenType;
import com.furkan.repositories.VerificationTokenRepository;
import com.furkan.services.IVerificationTokenService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class VerificationTokenServiceImpl implements IVerificationTokenService {

    private final VerificationTokenRepository verificationTokenRepository;

    @Override
    public VerificationToken createToken(User user, TokenType tokenType, int expiryMinutes) {
        verificationTokenRepository.invalidateAllByUserAndTokenType(
                user, tokenType, LocalDateTime.now()
        );

        VerificationToken token = VerificationToken.builder()
                .token(UUID.randomUUID().toString())
                .user(user)
                .tokenType(tokenType)
                .expiresAt(LocalDateTime.now().plusMinutes(expiryMinutes))
                .build();

        return verificationTokenRepository.save(token);
    }

    @Override
    public VerificationToken validateToken(String token, TokenType tokenType) {
        VerificationToken verificationToken = verificationTokenRepository.findByTokenAndTokenType(token, tokenType)
                .orElseThrow(() -> new RuntimeException("Invalid Token"));

        if (!verificationToken.isValid()) {
            if (verificationToken.isExpired()) {
                throw new RuntimeException("Token has expired");
            }
            throw new RuntimeException("Token has already been used");
        }

        return verificationToken;
    }

    @Override
    public void markAsUsed(VerificationToken token) {
        token.setUsedAt(LocalDateTime.now());
        verificationTokenRepository.save(token);
    }
}
