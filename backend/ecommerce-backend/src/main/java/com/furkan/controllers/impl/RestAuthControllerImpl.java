package com.furkan.controllers.impl;

import com.furkan.controllers.IRestAuthController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoLoginRequest;
import com.furkan.dto.request.DtoRegisterRequest;
import com.furkan.dto.response.DtoAuthResponse;
import com.furkan.services.IAuthService;
import com.furkan.utils.RootEntity;
import jakarta.servlet.http.HttpServletResponse;
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
    public RootEntity<DtoAuthResponse> register(@RequestBody DtoRegisterRequest request, HttpServletResponse response) {
        DtoAuthResponse auth = authService.register(request);

        setRefreshTokenCookie(response, auth.getRefreshToken(), Duration.ofDays(3));

        auth.setRefreshToken(null);
        return ok(auth);
    }

    @PostMapping("/login")
    @Override
    public RootEntity<DtoAuthResponse> login(@RequestBody DtoLoginRequest request, HttpServletResponse response) {
        DtoAuthResponse auth = authService.login(request);

        setRefreshTokenCookie(response, auth.getRefreshToken(), Duration.ofDays(3));

        auth.setRefreshToken(null);
        return ok(auth);

    }

    @PostMapping("/refresh")
    @Override
    public RootEntity<DtoAuthResponse> refresh(@CookieValue("refreshToken") String request, HttpServletResponse response) {
        DtoAuthResponse auth = authService.refresh(request);

        setRefreshTokenCookie(response, auth.getRefreshToken(), Duration.ofDays(3));

        auth.setRefreshToken(null);
        return ok(auth);
    }

    @PostMapping("/logout")
    @Override
    public RootEntity<Void> logout(@CookieValue("refreshToken") String request, HttpServletResponse response) {
        authService.logout(request);

        setRefreshTokenCookie(response, "", Duration.ZERO);

        return ok();
    }

    private void setRefreshTokenCookie(HttpServletResponse response, String value, Duration maxAge) {
        ResponseCookie cookie = ResponseCookie.from("refresh_token", value)
                .httpOnly(true)
                .secure(true)
                .sameSite("Strict")
                .path("/api/auth/refresh")
                .maxAge(maxAge)
                .build();
        response.addHeader(HttpHeaders.SET_COOKIE, cookie.toString());
    }
}
