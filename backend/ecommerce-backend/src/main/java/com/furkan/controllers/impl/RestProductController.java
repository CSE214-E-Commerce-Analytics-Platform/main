package com.furkan.controllers.impl;

import com.furkan.controllers.IRestProductController;
import com.furkan.controllers.RestBaseController;
import com.furkan.dto.request.DtoProductRequest;
import com.furkan.dto.response.DtoProduct;
import com.furkan.services.IProductService;
import com.furkan.utils.RootEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
public class RestProductController extends RestBaseController implements IRestProductController {

    private final IProductService productService;

    @PostMapping()
    @Override
    public RootEntity<DtoProduct> createProduct(@RequestBody DtoProductRequest input) {
        return ok(productService.createProduct(input));
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
    @Override
    public RootEntity<DtoProduct> updateProductById(@PathVariable Long id, @RequestBody DtoProductRequest input) {
        return ok(productService.updateProductById(id, input));
    }

    @DeleteMapping("/{id}")
    @Override
    public RootEntity<Void> deleteProductById(@PathVariable Long id) {
        productService.deleteProductById(id);
        return ok();
    }

    @GetMapping("/store/{storeId}")
    @Override
    public RootEntity<List<DtoProduct>> findAllByStoreId(@PathVariable Long storeId) {
        return ok(productService.findAllByStoreId(storeId));
    }
}
