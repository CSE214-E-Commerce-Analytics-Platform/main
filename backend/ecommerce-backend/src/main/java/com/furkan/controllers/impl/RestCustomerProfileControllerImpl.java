package com.furkan.controllers.impl;

import com.furkan.controllers.IRestCustomerProfileController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoCustomerProfileRequest;
import com.furkan.dto.response.DtoCustomerProfile;
import com.furkan.entities.User;
import com.furkan.enums.MembershipType;
import com.furkan.services.ICustomerProfileService;
import com.furkan.utils.RootEntity;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/customer-profiles")
@RequiredArgsConstructor
public class RestCustomerProfileControllerImpl extends RestBaseController implements IRestCustomerProfileController {

    private final ICustomerProfileService profileService;

    @GetMapping("/my-profile")
    @Override
    public RootEntity<DtoCustomerProfile> findProfileByUserId(@AuthenticationPrincipal UserDetails userDetails) {
        return ok(profileService.findProfileByUserId(getUserIdByToken(userDetails)));
    }

    @PutMapping("/my-profile/update")
    @Override
    public RootEntity<DtoCustomerProfile> updateProfileByUserId(@AuthenticationPrincipal UserDetails userDetails, @Valid @RequestBody DtoCustomerProfileRequest request) {
        return ok(profileService.updateProfileByUserId(getUserIdByToken(userDetails), request));
    }

    @PutMapping("/my-profile/upgrade-membership")
    @Override
    public RootEntity<DtoCustomerProfile> upgradeMembershipType(@AuthenticationPrincipal UserDetails userDetails, @RequestParam MembershipType type) {
        return ok(profileService.upgradeMembershipType(getUserIdByToken(userDetails), type));
    }

    private Long getUserIdByToken(UserDetails userDetails) {
        return ((User) userDetails).getId();
    }
}
