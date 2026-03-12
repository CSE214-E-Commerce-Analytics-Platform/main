package com.furkan.services;

import com.furkan.dto.request.DtoForgotPasswordRequest;
import com.furkan.dto.request.DtoLoginRequest;
import com.furkan.dto.request.DtoRegisterRequest;
import com.furkan.dto.request.DtoResetPasswordRequest;
import com.furkan.dto.response.DtoAuthResponse;

public interface IAuthService {

    DtoAuthResponse login(DtoLoginRequest request);

    void register(DtoRegisterRequest request);

    DtoAuthResponse refresh(String request);

    void logout(String refresh);

    void verifyEmail(String token);

    void forgotPassword(DtoForgotPasswordRequest request);

    void resetPassword(DtoResetPasswordRequest request);
}
