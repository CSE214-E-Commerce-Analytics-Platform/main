package com.furkan.controllers;

import com.furkan.dto.request.DtoUserRequest;
import com.furkan.dto.response.DtoUser;
import com.furkan.enums.RoleType;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;

public interface IRestUserController {

    RootEntity<DtoUser> findUserById(Long id);

    RootEntity<RestPageableEntity<DtoUser>> findAllUsers(RestPageableRequest request);

    RootEntity<DtoUser> findUserByEmail(String email);

    RootEntity<RestPageableEntity<DtoUser>> findAllUsersByRole(RoleType role, RestPageableRequest request);

    RootEntity<DtoUser> updateUserById(Long id, DtoUserRequest input);

    RootEntity<Void> deleteUserById(Long id);
}
