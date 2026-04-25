package com.furkan.services.impl;

import com.furkan.dto.request.DtoForgotPasswordRequest;
import com.furkan.dto.request.DtoLoginRequest;
import com.furkan.dto.request.DtoRegisterRequest;
import com.furkan.dto.request.DtoResetPasswordRequest;
import com.furkan.dto.response.DtoAuthResponse;
import com.furkan.entities.*;
import com.furkan.enums.AuthProvider;
import com.furkan.enums.MembershipType;
import com.furkan.enums.RoleType;
import com.furkan.enums.TokenType;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.CartRepository;
import com.furkan.repositories.CustomerProfileRepository;
import com.furkan.repositories.RefreshTokenRepository;
import com.furkan.repositories.UserRepository;
import com.furkan.security.JwtService;
import com.furkan.services.IAuthService;
import com.furkan.services.IEmailService;
import com.furkan.services.IVerificationTokenService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Transactional
public class AuthServiceImpl implements IAuthService {

    private final UserRepository userRepository;
    private final RefreshTokenRepository refreshTokenRepository;
    private final IVerificationTokenService verificationTokenService;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    private final AuthenticationManager authenticationManager;
    private final IEmailService emailService;
    private final CartRepository cartRepository;
    private final CustomerProfileRepository customerProfileRepository;

    @Override
    public DtoAuthResponse login(DtoLoginRequest request) {
        try {
            authenticationManager.authenticate(
                    new UsernamePasswordAuthenticationToken(request.getEmail(), request.getPassword()));
        } catch (DisabledException e) {
            throw new BaseException(new ErrorMessage(MessageType.ACCOUNT_IS_NOT_VERIFIED, null));
        } catch (BadCredentialsException e) {
            throw new BaseException(new ErrorMessage(MessageType.INVALID_EMAIL_OR_PASSWORD, null));
        }

        User user = userRepository.findByEmail(request.getEmail())
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, request.getEmail())));

        refreshTokenRepository.revokeAllByUser(user, LocalDateTime.now());

        RefreshToken refreshToken = new RefreshToken();
        refreshToken.setToken(jwtService.generateRefreshTokenValue());
        refreshToken.setUser(user);
        refreshToken.setExpiresAt(jwtService.getRefreshTokenExpiry());
        refreshTokenRepository.save(refreshToken);

        String accessToken = jwtService.generateAccessToken(user);

        return new DtoAuthResponse(accessToken, refreshToken.getToken(), user.getRoleType());
    }

    @Override
    public void register(DtoRegisterRequest request) {
        if (userRepository.existsByEmail(request.getEmail())) {
            throw new BaseException(new ErrorMessage(MessageType.EMAIL_ALREADY_EXISTS, request.getEmail()));
        }

        User user = new User();
        user.setEmail(request.getEmail());
        user.setPasswordHash(passwordEncoder.encode(request.getPassword()));
        user.setRoleType(RoleType.INDIVIDUAL); // default
        user.setProvider(AuthProvider.LOCAL);
        user.setActive(false);
        User savedUser = userRepository.save(user);

        CustomerProfile initializedProfile = new CustomerProfile();
        initializedProfile.setUser(savedUser);
        initializedProfile.setMembershipType(MembershipType.STANDARD);
        initializedProfile.setCreatedAt(LocalDateTime.now());
        initializedProfile.setUpdatedAt(LocalDateTime.now());
        if (request.getAge() > 0 && request.getAge() < 150) {
            initializedProfile.setAge(request.getAge());
        }
        if (request.getCity() != null && !request.getCity().isEmpty()) {
            initializedProfile.setCity(request.getCity());
        }
        if (request.getState() != null && !request.getState().isEmpty()) {
            initializedProfile.setState(request.getState());
        }
        if (request.getCountry() != null && !request.getCountry().isEmpty()) {
            initializedProfile.setCountry(request.getCountry());
        }
        customerProfileRepository.save(initializedProfile);

        Cart cart = new Cart();
        cart.setUser(savedUser);
        cart.setTotalPrice(BigDecimal.ZERO);
        cartRepository.save(cart);


        VerificationToken verificationToken = verificationTokenService
                .createToken(user, TokenType.EMAIL_VERIFICATION, 24 * 60);
        emailService.sendVerificationEmail(user.getEmail(), verificationToken.getToken());
    }

    @Override
    public DtoAuthResponse refresh(String refreshTokenValue) {
        RefreshToken stored = refreshTokenRepository
                .findByToken(refreshTokenValue)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.REFRESH_TOKEN_NOT_FOUND, null)));

        if (stored.isRevoked() || stored.isExpired()) {
            throw new BaseException(new ErrorMessage(MessageType.REFRESH_TOKEN_IS_EXPIRED, null));
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

    @Override
    public void verifyEmail(String token) {
        VerificationToken verificationToken = verificationTokenService
                .validateToken(token, TokenType.EMAIL_VERIFICATION);

        User user = verificationToken.getUser();
        user.setActive(true);
        userRepository.save(user);
        emailService.sendWelcomeEmail(user.getEmail());

        verificationTokenService.markAsUsed(verificationToken);
    }

    @Override
    public void forgotPassword(DtoForgotPasswordRequest request) {
        userRepository.findByEmail(request.getEmail())
                .ifPresent(user -> {
                    VerificationToken resetToken = verificationTokenService
                            .createToken(user, TokenType.PASSWORD_RESET, 15);
                    emailService.sendPasswordResetEmail(user.getEmail(), resetToken.getToken());
                });
    }

    @Override
    public void resetPassword(DtoResetPasswordRequest request) {
        VerificationToken verificationToken = verificationTokenService
                .validateToken(request.getToken(), TokenType.PASSWORD_RESET);

        User user = verificationToken.getUser();

        if (passwordEncoder.matches(request.getNewPassword(), user.getPasswordHash())) {
            throw new BaseException(new ErrorMessage(MessageType.SAME_PASSWORD, null));
        }

        user.setPasswordHash(passwordEncoder.encode(request.getNewPassword()));
        userRepository.save(user);

        verificationTokenService.markAsUsed(verificationToken);
    }
}
