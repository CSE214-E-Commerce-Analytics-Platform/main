package com.furkan.controllers.impl;

import com.furkan.controllers.IRestAddressController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoAddressRequest;
import com.furkan.dto.response.DtoAddress;
import com.furkan.entities.User;
import com.furkan.services.IAddressService;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/addresses")
@RequiredArgsConstructor
public class RestAddressControllerImpl extends RestBaseController implements IRestAddressController {

    private final IAddressService addressService;

    @PostMapping()
    @Override
    public RootEntity<DtoAddress> createAddress(@AuthenticationPrincipal UserDetails userDetails, @Valid @RequestBody DtoAddressRequest request) {
        return ok(addressService.createAddress(getUserIdFromToken(userDetails), request));
    }

    @GetMapping("/my-addresses")
    @Override
    public RootEntity<RestPageableEntity<DtoAddress>> findMyAddresses(@AuthenticationPrincipal UserDetails userDetails, @ModelAttribute RestPageableRequest request) {
        return ok(addressService.findMyAddresses(getUserIdFromToken(userDetails), request));
    }

    @PutMapping("/{addressId}")
    @Override
    public RootEntity<DtoAddress> updateAddress(@PathVariable Long addressId, @AuthenticationPrincipal UserDetails userDetails, @Valid @RequestBody DtoAddressRequest request) {
        return ok(addressService.updateAddress(addressId, getUserIdFromToken(userDetails), request));
    }

    @DeleteMapping("/{addressId}")
    @Override
    public RootEntity<Void> deleteAddress(@PathVariable Long addressId, @AuthenticationPrincipal UserDetails userDetails) {
        addressService.deleteAddress(addressId, getUserIdFromToken(userDetails));
        return ok();
    }

    private Long getUserIdFromToken(UserDetails userDetails) {
        return ((User) userDetails).getId();
    }
}
