package com.furkan.controllers;

import com.furkan.dto.request.DtoLoginRequest;
import com.furkan.dto.request.DtoRegisterRequest;
import com.furkan.dto.response.DtoAuthResponse;
import com.furkan.utils.RootEntity;
import jakarta.servlet.http.HttpServletResponse;

public interface IRestAuthController {

    RootEntity<DtoAuthResponse> register(DtoRegisterRequest request, HttpServletResponse response);

    RootEntity<DtoAuthResponse> login(DtoLoginRequest request, HttpServletResponse response);

    RootEntity<DtoAuthResponse> refresh(String request, HttpServletResponse response);

    RootEntity<Void> logout(String request, HttpServletResponse response);
}
