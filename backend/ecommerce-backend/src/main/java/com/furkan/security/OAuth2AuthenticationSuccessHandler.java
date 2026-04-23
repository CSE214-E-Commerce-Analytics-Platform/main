package com.furkan.security;

import com.furkan.entities.RefreshToken;
import com.furkan.entities.User;
import com.furkan.repositories.RefreshTokenRepository;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class OAuth2AuthenticationSuccessHandler implements AuthenticationSuccessHandler {

    private final JwtService jwtService;
    private final RefreshTokenRepository refreshTokenRepository;

    @Override
    public void onAuthenticationSuccess(HttpServletRequest request, HttpServletResponse response,
                                        Authentication authentication) throws IOException {
        OAuth2UserPrincipal principal = (OAuth2UserPrincipal) authentication.getPrincipal();
        User user = principal.getUser();

        refreshTokenRepository.revokeAllByUser(user, LocalDateTime.now());

        RefreshToken refreshToken = new RefreshToken();
        refreshToken.setToken(jwtService.generateRefreshTokenValue());
        refreshToken.setUser(user);
        refreshToken.setExpiresAt(jwtService.getRefreshTokenExpiry());
        refreshTokenRepository.save(refreshToken);

        ResponseCookie cookie = ResponseCookie.from("refresh_token", refreshToken.getToken())
                .httpOnly(true)
                .secure(false)   // set true in production
                .sameSite("Lax") // Lax is required for cross-site OAuth2 redirects
                .path("/")
                .maxAge(Duration.ofDays(3))
                .build();
        response.addHeader(HttpHeaders.SET_COOKIE, cookie.toString());

        String accessToken = jwtService.generateAccessToken(user);
        String role = user.getRoleType().name();

        response.sendRedirect("http://localhost:4200/oauth2/callback?token=" + accessToken + "&role=" + role);
    }
}
