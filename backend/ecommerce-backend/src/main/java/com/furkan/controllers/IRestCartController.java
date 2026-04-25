package com.furkan.controllers;

import com.furkan.dto.request.DtoCartItemRequest;
import com.furkan.dto.response.DtoCart;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

public interface IRestCartController {

    RootEntity<DtoCart> findCartByUserId(UserDetails userDetails);

    RootEntity<DtoCart> addItemToCart(UserDetails userDetails, DtoCartItemRequest itemRequest);

    RootEntity<DtoCart> updateQuantity(UserDetails userDetails, Long itemId, int quantity);

    RootEntity<DtoCart> removeItem(UserDetails userDetails, Long itemId);

    RootEntity<Void> clearCart(UserDetails userDetails);

    RootEntity<RestPageableEntity<DtoCart>> findAllCarts(RestPageableRequest request);

    RootEntity<DtoCart> findCartByUserId(Long userId);

    RootEntity<Void> adminDeleteCart(Long cartId);
}
