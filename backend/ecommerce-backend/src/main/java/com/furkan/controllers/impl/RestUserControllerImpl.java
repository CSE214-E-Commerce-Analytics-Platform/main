package com.furkan.controllers.impl;

import com.furkan.controllers.IRestUserController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoUserRequest;
import com.furkan.dto.response.DtoUser;
import com.furkan.services.IUserService;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class RestUserControllerImpl extends RestBaseController implements IRestUserController {

    private final IUserService userService;

    @GetMapping("/{id}")
    @Override
    public RootEntity<DtoUser> findUserById(@PathVariable Long id) {
        return ok(userService.findUserById(id));
    }

    @GetMapping()
    @Override
    public RootEntity<List<DtoUser>> findAllUsers() {
        return ok(userService.findAllUsers());
    }

    @GetMapping("/email/{email}")
    @Override
    public RootEntity<DtoUser> findUserByEmail(@PathVariable String email) {
        return ok(userService.findUserByEmail(email));
    }

    @PutMapping("/{id}")
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
