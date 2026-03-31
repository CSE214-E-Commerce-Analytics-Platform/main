package com.furkan.controllers.impl;

import com.furkan.controllers.IRestProductController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoProductRequest;
import com.furkan.dto.response.DtoProduct;
import com.furkan.entities.User;
import com.furkan.services.IProductService;
import com.furkan.utils.RootEntity;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
public class RestProductControllerImpl extends RestBaseController implements IRestProductController {

    private final IProductService productService;

    @PostMapping()
    @PreAuthorize("hasRole('CORPORATE')")
    @Override
    public RootEntity<DtoProduct> createProduct(@Valid @RequestBody DtoProductRequest input, @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = ((User) userDetails).getId();
        return ok(productService.createProduct(input, userId));
    }

    @GetMapping("/{id}")
    @Override
    public RootEntity<DtoProduct> findProductById(@PathVariable Long id) {
        return ok(productService.findProductById(id));
    }

    @GetMapping()
    @Override
    public RootEntity<List<DtoProduct>> findAllProducts() {
        return ok(productService.findAllProducts());
    }

    @PutMapping("/{id}")
    @PreAuthorize("@securityService.isProductOwner(authentication, #id)")
    @Override
    public RootEntity<DtoProduct> updateProductById(@PathVariable Long id, @Valid @RequestBody DtoProductRequest input, @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = ((User) userDetails).getId();
        return ok(productService.updateProductById(id, input, userId));
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN') or @securityService.isProductOwner(authentication, #id)")
    @Override
    public RootEntity<Void> deleteProductById(@PathVariable Long id, @AuthenticationPrincipal UserDetails userDetails) {
        Long userId = ((User) userDetails).getId();
        productService.deleteProductById(id, userId);
        return ok();
    }

    @GetMapping("/store/{storeId}")
    @Override
    public RootEntity<List<DtoProduct>> findAllByStoreId(@PathVariable Long storeId) {
        return ok(productService.findAllByStoreId(storeId));
    }
}
