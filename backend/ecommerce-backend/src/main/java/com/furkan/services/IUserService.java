package com.furkan.services;

import com.furkan.dto.request.DtoUserRequest;
import com.furkan.dto.response.DtoUser;
import com.furkan.enums.RoleType;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;

public interface IUserService {

    DtoUser findUserById(Long id);

    RestPageableEntity<DtoUser> findAllUsers(RestPageableRequest request);

    DtoUser findUserByEmail(String email);

    RestPageableEntity<DtoUser> findAllUsersByRole(RoleType role, RestPageableRequest request);

    DtoUser updateUserById(Long id, DtoUserRequest input);

    void deleteUserById(Long id);
}
