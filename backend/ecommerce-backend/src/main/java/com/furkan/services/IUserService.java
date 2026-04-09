package com.furkan.services;

import com.furkan.dto.request.DtoLoginRequest;
import com.furkan.dto.request.DtoUserRequest;
import com.furkan.dto.response.DtoUser;
import com.furkan.enums.RoleType;

import java.util.List;

public interface IUserService {

    DtoUser findUserById(Long id);

    List<DtoUser> findAllUsers();

    DtoUser findUserByEmail(String email);

    List<DtoUser> findAllUsersByRole(RoleType role);

    DtoUser updateUserById(Long id, DtoUserRequest input);

    void deleteUserById(Long id);
}
