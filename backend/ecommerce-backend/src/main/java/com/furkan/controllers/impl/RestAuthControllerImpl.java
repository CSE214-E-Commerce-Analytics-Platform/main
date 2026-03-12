package com.furkan.controllers.impl;

import com.furkan.controllers.IRestAuthController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoForgotPasswordRequest;
import com.furkan.dto.request.DtoLoginRequest;
import com.furkan.dto.request.DtoRegisterRequest;
import com.furkan.dto.request.DtoResetPasswordRequest;
import com.furkan.dto.response.DtoAuthResponse;
import com.furkan.services.IAuthService;
import com.furkan.utils.RootEntity;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class RestAuthControllerImpl extends RestBaseController implements IRestAuthController {

    private final IAuthService authService;

    @PostMapping("/register")
    @Override
    public RootEntity<String> register(@Valid @RequestBody DtoRegisterRequest request) {
        authService.register(request);
        return ok("Verification email sent. Please check your inbox.");
    }

    @PostMapping("/login")
    @Override
    public RootEntity<DtoAuthResponse> login(@Valid @RequestBody DtoLoginRequest request, HttpServletResponse response) {
        DtoAuthResponse auth = authService.login(request);

        setRefreshTokenCookie(response, auth.getRefreshToken(), Duration.ofDays(3));

        auth.setRefreshToken(null);
        return ok(auth);

    }

    @PostMapping("/refresh")
    @Override
    public RootEntity<DtoAuthResponse> refresh(@CookieValue("refresh_token") String request, HttpServletResponse response) {
        DtoAuthResponse auth = authService.refresh(request);

        setRefreshTokenCookie(response, auth.getRefreshToken(), Duration.ofDays(3));

        auth.setRefreshToken(null);
        return ok(auth);
    }

    @PostMapping("/logout")
    @Override
    public RootEntity<Void> logout(@CookieValue("refresh_token") String request, HttpServletResponse response) {
        authService.logout(request);

        setRefreshTokenCookie(response, "", Duration.ZERO);

        return ok();
    }

    @GetMapping("/verify-email")
    @Override
    public RootEntity<String> verifyEmail(@Valid @RequestParam String token) {
        authService.verifyEmail(token);
        return ok("Email verified successfully. You can now login.");
    }

    @PostMapping("/forgot-password")
    @Override
    public RootEntity<String> forgotPassword(@Valid @RequestBody DtoForgotPasswordRequest request) {
        authService.forgotPassword(request);
        return ok("If this email exists, a reset link has been sent.");
    }

    @PostMapping("/reset-password")
    @Override
    public RootEntity<String> resetPassword(@Valid @RequestBody DtoResetPasswordRequest request) {
        authService.resetPassword(request);
        return ok("Password reset successfully. You can now login.");
    }

    private void setRefreshTokenCookie(HttpServletResponse response, String value, Duration maxAge) {
        ResponseCookie cookie = ResponseCookie.from("refresh_token", value)
                .httpOnly(true)
                .secure(true) // geliştirmede false, productionda true
                .sameSite("Strict") // geliştirmede Lax, productionda Strict
                .path("/")  // geliştirmede cookie her yerde görünebilsin diye "/" bırak, productionda "/api/auth" a çek
                .maxAge(maxAge)
                .build();
        response.addHeader(HttpHeaders.SET_COOKIE, cookie.toString());
    }
}
