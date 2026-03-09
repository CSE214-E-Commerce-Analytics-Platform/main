package com.furkan.services.impl;

import com.furkan.dto.request.DtoLoginRequest;
import com.furkan.dto.request.DtoRegisterRequest;
import com.furkan.dto.response.DtoAuthResponse;
import com.furkan.entities.RefreshToken;
import com.furkan.entities.User;
import com.furkan.enums.RoleType;
import com.furkan.repositories.RefreshTokenRepository;
import com.furkan.repositories.UserRepository;
import com.furkan.security.JwtService;
import com.furkan.services.IAuthService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Transactional
public class AuthServiceImpl implements IAuthService {

    private final UserRepository userRepository;
    private final RefreshTokenRepository refreshTokenRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    private final AuthenticationManager authenticationManager;

    @Override
    public DtoAuthResponse login(DtoLoginRequest request) {
        authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(request.getEmail(), request.getPassword()));

        User user = userRepository.findByEmail(request.getEmail()).orElseThrow();

        String accessToken = jwtService.generateAccessToken(user);

        refreshTokenRepository.revokeAllByUser(user, LocalDateTime.now());

        RefreshToken refreshToken = new RefreshToken();
        refreshToken.setToken(jwtService.generateRefreshTokenValue());
        refreshToken.setUser(user);
        refreshToken.setExpiresAt(jwtService.getRefreshTokenExpiry());
        refreshTokenRepository.save(refreshToken);

        return new DtoAuthResponse(accessToken, refreshToken.getToken(), user.getRoleType());
    }

    @Override
    public DtoAuthResponse register(DtoRegisterRequest request) {
        if (userRepository.existsByEmail(request.getEmail())) {
            throw new RuntimeException("Email already in use");
        }

        User user = new User();
        user.setEmail(request.getEmail());
        user.setPasswordHash(passwordEncoder.encode(request.getPassword()));
        user.setRoleType(RoleType.INDIVIDUAL);  // default
        user.setGender(request.getGender());
        user.setActive(true);

        userRepository.save(user);

        String accessToken = jwtService.generateAccessToken(user);

        RefreshToken refreshToken = new RefreshToken();
        refreshToken.setToken(jwtService.generateRefreshTokenValue());
        refreshToken.setUser(user);
        refreshToken.setExpiresAt(jwtService.getRefreshTokenExpiry());
        refreshTokenRepository.save(refreshToken);

        return new DtoAuthResponse(accessToken, refreshToken.getToken(), user.getRoleType());
    }

    @Override
    public DtoAuthResponse refresh(String refreshTokenValue) {
        RefreshToken stored = refreshTokenRepository
                .findByToken(refreshTokenValue)
                .orElseThrow(() -> new RuntimeException("Refresh token not found"));

        if (stored.isRevoked() || stored.isExpired()) {
            throw new RuntimeException("Refresh token expired or revoked");
        }

        RefreshToken newRefreshToken = new RefreshToken();
        newRefreshToken.setToken(jwtService.generateRefreshTokenValue());
        newRefreshToken.setUser(stored.getUser());
        newRefreshToken.setExpiresAt(jwtService.getRefreshTokenExpiry());
        refreshTokenRepository.save(newRefreshToken);

        stored.setRevokedAt(LocalDateTime.now());
        stored.setReplacedBy(newRefreshToken);
        refreshTokenRepository.save(stored);

        String newAccessToken = jwtService.generateAccessToken(stored.getUser());

        return new DtoAuthResponse(newAccessToken, newRefreshToken.getToken(), stored.getUser().getRoleType());
    }

    @Override
    public void logout(String refreshTokenValue) {
        refreshTokenRepository.findByToken(refreshTokenValue)
                .ifPresent(t -> {
                    t.setRevokedAt(LocalDateTime.now());
                    refreshTokenRepository.save(t);
                });
    }
}
