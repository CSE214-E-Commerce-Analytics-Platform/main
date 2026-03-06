package com.furkan.services.impl;

import com.furkan.dto.request.DtoUserRequest;
import com.furkan.dto.response.DtoUser;
import com.furkan.entities.User;
import com.furkan.enums.RoleType;
import com.furkan.repositories.UserRepository;
import com.furkan.services.IUserService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class UserService implements IUserService {

    private final UserRepository userRepository;

    private DtoUser dtoTransformation(User user) {
        DtoUser dtoUser = new DtoUser();
        BeanUtils.copyProperties(user, dtoUser);
        dtoUser.setRoleType(user.getRoleType().name());
        return dtoUser;
    }

    @Override
    @Transactional
    public DtoUser createUser(DtoUserRequest input) {
        User user = new User();

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
    public DtoUser findUserById(Long id) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("User not found!"));

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
                .orElseThrow(() -> new RuntimeException("User not found!"));
        return dtoTransformation(user);
    }

    @Override
    @Transactional
    public DtoUser updateUserById(Long id, DtoUserRequest input) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("User not found!"));

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
                .orElseThrow(() -> new RuntimeException("User not found!"));
        userRepository.delete(user);
    }
}
