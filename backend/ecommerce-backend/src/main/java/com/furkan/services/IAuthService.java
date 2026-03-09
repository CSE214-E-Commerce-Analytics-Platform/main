package com.furkan.services;

import com.furkan.dto.request.DtoLoginRequest;
import com.furkan.dto.request.DtoRegisterRequest;
import com.furkan.dto.response.DtoAuthResponse;

public interface IAuthService {

    DtoAuthResponse login(DtoLoginRequest request);

    DtoAuthResponse register(DtoRegisterRequest request);

    DtoAuthResponse refresh(String request);

    void logout(String refresh);
}
