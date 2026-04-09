package com.furkan.controllers.impl;

import com.furkan.controllers.IRestUserController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoUserRequest;
import com.furkan.dto.response.DtoUser;
import com.furkan.entities.User;
import com.furkan.services.IUserService;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class RestUserControllerImpl extends RestBaseController implements IRestUserController {

    private final IUserService userService;

    @GetMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<DtoUser> findUserById(@PathVariable Long id) {
        return ok(userService.findUserById(id));
    }

    @GetMapping()
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<List<DtoUser>> findAllUsers() {
        return ok(userService.findAllUsers());
    }

    @GetMapping("/email/{email}")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<DtoUser> findUserByEmail(@PathVariable String email) {
        return ok(userService.findUserByEmail(email));
    }

    @PutMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<DtoUser> updateUserById(@PathVariable Long id, @RequestBody DtoUserRequest input) {
        return ok(userService.updateUserById(id, input));
    }

    @DeleteMapping("/{id}")
    @Override
    public RootEntity<Void> deleteUserById(@PathVariable Long id) {
        userService.deleteUserById(id);
        return ok();
    }
}
