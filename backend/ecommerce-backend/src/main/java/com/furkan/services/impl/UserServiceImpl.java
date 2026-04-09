package com.furkan.services.impl;

import com.furkan.dto.request.DtoUserRequest;
import com.furkan.dto.response.DtoUser;
import com.furkan.entities.User;
import com.furkan.enums.RoleType;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.UserRepository;
import com.furkan.services.IUserService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class UserServiceImpl implements IUserService {

    private final UserRepository userRepository;

    private DtoUser dtoTransformation(User user) {
        DtoUser dtoUser = new DtoUser();
        BeanUtils.copyProperties(user, dtoUser);
        dtoUser.setRoleType(user.getRoleType().name());
        if (user.getRoleType().equals(RoleType.CORPORATE) && user.getStore() != null) {
            dtoUser.setStoreId(user.getStore().getId());
        }
        return dtoUser;
    }

    @Override
    public DtoUser findUserById(Long id) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, id.toString())));

        return dtoTransformation(user);
    }

    @Override
    public List<DtoUser> findAllUsers() {
        return userRepository.findAll().stream()
                .map(this::dtoTransformation)
                .toList();
    }

    @Override
    public DtoUser findUserByEmail(String email) {
        User user = userRepository.findByEmail(email)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND_BY_EMAIL, email)));
        return dtoTransformation(user);
    }

    @Override
    public List<DtoUser> findAllUsersByRole(RoleType role) {
        return userRepository.findAllByRoleType(role).stream()
                .map(this::dtoTransformation)
                .toList();
    }

    @Override
    @Transactional
    public DtoUser updateUserById(Long id, DtoUserRequest input) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, id.toString())));

        user.setEmail(input.getEmail());
        if (input.getGender() != null) {
            user.setGender(input.getGender());
        }
        user.setPasswordHash(input.getPassword());
        user.setRoleType(RoleType.valueOf(input.getRoleType().toUpperCase()));

        userRepository.save(user);

        return dtoTransformation(user);
    }

    @Override
    @Transactional
    public void deleteUserById(Long id) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, id.toString())));
        userRepository.delete(user);
    }
}
