package com.furkan.services;

import com.furkan.entities.User;
import com.furkan.entities.VerificationToken;
import com.furkan.enums.TokenType;

public interface IVerificationTokenService {

    VerificationToken createToken(User user, TokenType tokenType, int expiryMinutes);

    VerificationToken validateToken(String token, TokenType tokenType);

    void markAsUsed(VerificationToken token);
}
