package com.furkan.services.impl;

import com.furkan.dto.request.DtoCartItemRequest;
import com.furkan.dto.response.DtoCart;
import com.furkan.dto.response.DtoCartItem;
import com.furkan.entities.Cart;
import com.furkan.entities.CartItem;
import com.furkan.entities.Product;
import com.furkan.entities.User;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.CartRepository;
import com.furkan.repositories.ProductRepository;
import com.furkan.repositories.UserRepository;
import com.furkan.services.ICartService;
import com.furkan.utils.PagerUtil;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class CartServiceImpl implements ICartService {

    private final CartRepository cartRepository;
    private final ProductRepository productRepository;
    private final UserRepository userRepository;

    @Override
    public DtoCart findCartByUserId(Long userId) {
        Cart cart = getCart(userId);
        return dtoCartMapper(cart);
    }

    @Override
    public RestPageableEntity<DtoCart> findAllCarts(RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Cart> cartPage = cartRepository.findAll(pageable);

        List<DtoCart> dtoList = cartPage.getContent().stream()
                .map(this::dtoCartMapper)
                .toList();

        return PagerUtil.toPageableResponse(cartPage, dtoList);
    }

    @Override
    @Transactional
    public DtoCart addItemToCart(Long userId, DtoCartItemRequest itemRequest) {
        Cart cart = getCart(userId);
        Product product = getProduct(itemRequest.getProductId());

        updateOrAddCartItem(cart, product, itemRequest.getQuantity());

        calculateAndSetCartTotals(cart);

        return dtoCartMapper(cartRepository.save(cart));
    }


    @Override
    @Transactional
    public DtoCart updateQuantity(Long userId, Long itemId, int quantity) {
        Cart cart = getCart(userId);

        CartItem itemToUpdate = cart.getCartItems().stream()
                .filter(item -> item.getId().equals(itemId))
                .findFirst()
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.ITEM_NOT_FOUND, itemId.toString())));

        if (quantity <= 0) {
            cart.getCartItems().remove(itemToUpdate);
        } else {
            itemToUpdate.setQuantity(quantity);
        }

        calculateAndSetCartTotals(cart);
        return dtoCartMapper(cartRepository.save(cart));
    }

    @Override
    @Transactional
    public DtoCart removeItem(Long userId, Long itemId) {
        Cart cart = getCart(userId);

        cart.getCartItems().removeIf(item -> item.getId().equals(itemId));

        calculateAndSetCartTotals(cart);
        return dtoCartMapper(cartRepository.save(cart));
    }

    @Override
    @Transactional
    public void clearCart(Long userId) {
        Cart cart = getCart(userId);
        cart.getCartItems().clear();
        cart.setTotalPrice(BigDecimal.ZERO);
        cartRepository.save(cart);
    }

    @Override
    @Transactional
    public void adminDeleteCart(Long cartId) {
        Cart cart = getCart(cartId);
        cartRepository.delete(cart);
    }

    @Override
    @Transactional
    public Cart findEntityCartByUserId(Long userId) {
        return cartRepository.findByUserId(userId)
                .orElseGet(() -> {
                    User user = userRepository.findById(userId)
                            .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, userId.toString())));

                    Cart newCart = new Cart();
                    newCart.setUser(user);
                    newCart.setTotalPrice(BigDecimal.ZERO);
                    newCart.setCartItems(new ArrayList<>());
                    return cartRepository.save(newCart);
                });
    }

    private Cart getCart(Long userId) {
        return cartRepository.findByUserId(userId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, userId.toString())));
    }

    private Product getProduct(Long productId) {
        return productRepository.findById(productId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.PRODUCT_NOT_FOUND, productId.toString())));
    }

    private void updateOrAddCartItem(Cart cart, Product product, int quantity) {
        Optional<CartItem> existingItem = cart.getCartItems().stream()
                .filter(item -> item.getProduct().getId().equals(product.getId()))
                .findFirst();

        if (existingItem.isPresent()) {
            existingItem.get().setQuantity(existingItem.get().getQuantity() + quantity);
        } else {
            CartItem newItem = new CartItem();
            newItem.setQuantity(quantity);
            newItem.setCart(cart);
            newItem.setProduct(product);
            cart.getCartItems().add(newItem);
        }
    }

    private void calculateAndSetCartTotals(Cart cart) {
        BigDecimal total = cart.getCartItems().stream()
                .map(item -> item.getProduct().getUnitPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        cart.setTotalPrice(total);
    }

    private DtoCart dtoCartMapper(Cart cart) {
        DtoCart dtoCart = new DtoCart();
        BeanUtils.copyProperties(cart, dtoCart);
        dtoCart.setUserId(cart.getUser().getId());

        int totalItems = 0;
        for (CartItem item : cart.getCartItems()) {
            dtoCart.getItems().add(dtoCartItemMapper(item));
            totalItems += item.getQuantity();
        }

        dtoCart.setTotalItems(totalItems);
        return dtoCart;
    }

    private DtoCartItem dtoCartItemMapper(CartItem cartItem) {
        DtoCartItem dtoCartItem = new DtoCartItem();
        BeanUtils.copyProperties(cartItem, dtoCartItem);

        dtoCartItem.setProductId(cartItem.getId());
        dtoCartItem.setProductName(cartItem.getProduct().getName());
        dtoCartItem.setProductPrice(cartItem.getProduct().getUnitPrice());
        dtoCartItem.setProductImageUrl(cartItem.getProduct().getImageUrl());

        dtoCartItem.setTotalLinePrice(cartItem.getProduct().getUnitPrice()
                .multiply(BigDecimal.valueOf(cartItem.getQuantity())));

        return dtoCartItem;
    }
}
