package com.furkan.controllers.impl;

import com.furkan.controllers.IRestStoreController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoStoreRequest;
import com.furkan.dto.response.DtoStore;
import com.furkan.services.IStoreService;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import com.furkan.entities.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/stores")
@RequiredArgsConstructor
public class RestStoreControllerImpl extends RestBaseController implements IRestStoreController {

    private final IStoreService storeService;

    @PostMapping()
    @PreAuthorize("hasRole('CORPORATE')")
    @Override
    public RootEntity<DtoStore> createStore(@RequestBody DtoStoreRequest input,
                                            @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = ((User) userDetails).getId();
        return ok(storeService.createStore(input, userId));
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasAnyRole('ADMIN', 'CORPORATE', 'INDIVIDUAL')")
    @Override
    public RootEntity<DtoStore> findStoreById(@PathVariable Long id) {
        return ok(storeService.findStoreById(id));
    }

    @GetMapping()
    @Override
    public RootEntity<RestPageableEntity<DtoStore>> findAllStores(@ModelAttribute RestPageableRequest request) {
        return ok(storeService.findAllStores(request));
    }

    @PutMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN') or @securityService.isStoreOwner(authentication, #id)")
    @Override
    public RootEntity<DtoStore> updateStoreById(@PathVariable Long id, @RequestBody DtoStoreRequest input, @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = ((User) userDetails).getId();

        return ok(storeService.updateStoreById(id, input, userId));
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<Void> deleteStoreById(@PathVariable Long id, @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = ((User) userDetails).getId();

        storeService.deleteStoreById(id, userId);
        return ok();
    }

    @GetMapping("/my-stores")
    @PreAuthorize("hasRole('CORPORATE')")
    @Override
    public RootEntity<RestPageableEntity<DtoStore>> findMyStores(@AuthenticationPrincipal UserDetails userDetails, @ModelAttribute RestPageableRequest request) {
        Long userId = ((User) userDetails).getId();
        return ok(storeService.findMyStores(userId, request));
    }
}
