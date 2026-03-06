package com.furkan.controllers;

import com.furkan.dto.request.DtoLoginRequest;
import com.furkan.dto.request.DtoUserRequest;
import com.furkan.dto.response.DtoUser;
import com.furkan.utils.RootEntity;

import java.util.List;

public interface IRestUserController {

    RootEntity<DtoUser> createUser(DtoUserRequest input);

    RootEntity<DtoUser> findUserById(Long id);

    RootEntity<List<DtoUser>> findAllUsers();

    RootEntity<DtoUser> findUserByEmail(String email);

    RootEntity<DtoUser> updateUserById(Long id, DtoUserRequest input);

    RootEntity<Void> deleteUserById(Long id);

    RootEntity<DtoUser> login(DtoLoginRequest input);
}
