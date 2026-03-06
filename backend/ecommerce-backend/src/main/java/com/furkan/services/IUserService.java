package com.furkan.services;

import com.furkan.dto.request.DtoUserRequest;
import com.furkan.dto.response.DtoUser;

import java.util.List;

public interface IUserService {

    DtoUser createUser(DtoUserRequest input);

    DtoUser findUserById(Long id);

    List<DtoUser> findAllUsers();

    DtoUser findUserByEmail(String email);

    DtoUser updateUserById(Long id, DtoUserRequest input);

    void deleteUserById(Long id);
}
