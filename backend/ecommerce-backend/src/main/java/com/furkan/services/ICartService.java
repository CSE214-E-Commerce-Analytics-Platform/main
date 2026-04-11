package com.furkan.services;

import com.furkan.dto.request.DtoCartItemRequest;
import com.furkan.dto.response.DtoCart;

import java.util.List;

public interface ICartService {


    DtoCart findCartByUserId(Long userId);

    DtoCart addItemToCart(Long userId, DtoCartItemRequest itemRequest);

    DtoCart updateQuantity(Long userId, Long itemId, int quantity);

    DtoCart removeItem(Long userId, Long itemId);

    void clearCart(Long userId);

    List<DtoCart> findAllCarts();

    void adminDeleteCart(Long cartId);
}
