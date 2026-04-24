package com.furkan.controllers.impl;

import com.furkan.controllers.IRestCartController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoCartItemRequest;
import com.furkan.dto.response.DtoCart;
import com.furkan.entities.User;
import com.furkan.services.ICartService;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/carts")
@RequiredArgsConstructor
public class RestCartControllerImpl extends RestBaseController implements IRestCartController {

    private final ICartService cartService;

    @GetMapping("/my-cart")
    @Override
    public RootEntity<DtoCart> findCartByUserId(@AuthenticationPrincipal UserDetails userDetails) {
        return ok(cartService.findCartByUserId(getUserIdByToken(userDetails)));
    }

    @PostMapping("/add-item")
    @Override
    public RootEntity<DtoCart> addItemToCart(@AuthenticationPrincipal UserDetails userDetails, @RequestBody DtoCartItemRequest itemRequest) {
        return ok(cartService.addItemToCart(getUserIdByToken(userDetails), itemRequest));
    }

    @PutMapping("/items/{itemId}/quantity/{quantity}")
    @Override
    public RootEntity<DtoCart> updateQuantity(@AuthenticationPrincipal UserDetails userDetails, @PathVariable Long itemId, @PathVariable int quantity) {
        return ok(cartService.updateQuantity(getUserIdByToken(userDetails), itemId, quantity));
    }

    @DeleteMapping("/items/{itemId}")
    @Override
    public RootEntity<DtoCart> removeItem(@AuthenticationPrincipal UserDetails userDetails,  @PathVariable Long itemId) {
        return ok(cartService.removeItem(getUserIdByToken(userDetails), itemId));
    }

    @DeleteMapping("/clear")
    @Override
    public RootEntity<Void> clearCart(@AuthenticationPrincipal UserDetails userDetails) {
        cartService.clearCart(getUserIdByToken(userDetails));
        return ok();
    }

    @GetMapping("/admin/all")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<RestPageableEntity<DtoCart>> findAllCarts(@ModelAttribute RestPageableRequest request) {
        return ok(cartService.findAllCarts(request));
    }

    @GetMapping("/admin/user/{userId}")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<DtoCart> findCartByUserId(@PathVariable Long userId) {
        return ok(cartService.findCartByUserId(userId));
    }

    @DeleteMapping("/admin/{cartId}")
    @PreAuthorize("hasRole('ADMIN')")
    @Override
    public RootEntity<Void> adminDeleteCart(Long cartId) {
        cartService.adminDeleteCart(cartId);
        return ok();
    }

    private Long getUserIdByToken(UserDetails userDetails) {
        return ((User) userDetails).getId();
    }
}
