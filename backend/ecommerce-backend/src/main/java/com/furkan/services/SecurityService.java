package com.furkan.services;

import com.furkan.entities.User;
import com.furkan.repositories.ProductRepository;
import com.furkan.repositories.StoreRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

@Service("securityService")
@RequiredArgsConstructor
public class SecurityService {

    private final StoreRepository storeRepository;
    private final ProductRepository productRepository;

    public boolean isStoreOwner(Authentication authentication, Long storeId) {
        User currentUser = (User) authentication.getPrincipal();
        return storeRepository.findByIdAndOwnerId(storeId, currentUser.getId()).isPresent();
    }

    public boolean isProductOwner(Authentication authentication, Long productId) {
        User currentUser = (User) authentication.getPrincipal();
        return productRepository.findByIdAndStoreOwnerId(productId, currentUser.getId()).isPresent();
    }
}
