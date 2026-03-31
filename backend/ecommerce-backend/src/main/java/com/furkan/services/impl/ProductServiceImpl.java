package com.furkan.services.impl;

import com.furkan.dto.request.DtoProductRequest;
import com.furkan.dto.response.DtoProduct;
import com.furkan.entities.Category;
import com.furkan.entities.Product;
import com.furkan.entities.Store;
import com.furkan.repositories.CategoryRepository;
import com.furkan.repositories.ProductRepository;
import com.furkan.repositories.StoreRepository;
import com.furkan.services.IProductService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ProductServiceImpl implements IProductService {

    private final ProductRepository productRepository;
    private final StoreRepository storeRepository;
    private final CategoryRepository categoryRepository;

    private DtoProduct dtoTransformation(Product product) {
        DtoProduct dto = new DtoProduct();
        BeanUtils.copyProperties(product, dto);

        if (product.getStore() != null) dto.setStoreId(product.getStore().getId());
        if (product.getCategory() != null) dto.setCategoryName(product.getCategory().getName());

        return dto;
    }

    @Override
    @Transactional
    public DtoProduct createProduct(DtoProductRequest input, Long authenticatedUserId) {
        if (productRepository.existsBySku(input.getSku())) {
            throw new RuntimeException("This SKU already exists!");
        }

        Store store = storeRepository.findById(input.getStoreId())
                .orElseThrow(() -> new RuntimeException("Store not found!"));

        Product product = new Product();
        BeanUtils.copyProperties(input, product);
        product.setStore(store);

        if (input.getCategoryId() != null) {
            Category category = categoryRepository.findById(input.getCategoryId())
                    .orElseThrow(() -> new RuntimeException("Category not found!"));
            product.setCategory(category);
        }

        return dtoTransformation(productRepository.save(product));
    }

    private Product getProductIfOwner(Long productId, Long userId) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new RuntimeException("Product not found!"));

        if (!product.getStore().getOwner().getId().equals(userId)) {
            throw new RuntimeException("Unauthorized transaction! This product does not belong in your store.");
        }

        return product;
    }

    @Override
    public DtoProduct findProductById(Long id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found!"));
        return dtoTransformation(product);
    }

    @Override
    public List<DtoProduct> findAllProducts() {
        return productRepository.findAll().stream()
                .map(this::dtoTransformation)
                .toList();
    }

    @Override
    @Transactional
    public DtoProduct updateProductById(Long id, DtoProductRequest input, Long authenticatedUserId) {
        Product product = getProductIfOwner(id, authenticatedUserId);

        if (!product.getSku().equals(input.getSku()) && productRepository.existsBySku(input.getSku())) {
            throw new RuntimeException("This SKU already exists!");
        }

        product.setName(input.getName());
        product.setDescription(input.getDescription());
        product.setUnitPrice(input.getUnitPrice());
        product.setStockQuantity(input.getStockQuantity());
        product.setImageUrl(input.getImageUrl());
        product.setSku(input.getSku());

        if (input.getCategoryId() != null && (product.getCategory() == null || !product.getCategory().getId().equals(input.getCategoryId()))) {
            Category category = categoryRepository.findById(input.getCategoryId())
                    .orElseThrow(() -> new RuntimeException("Category not found!"));
            product.setCategory(category);
        }

        return dtoTransformation(productRepository.save(product));
    }

    @Override
    @Transactional
    public void deleteProductById(Long id, Long authenticatedUserId) {
        Product product = getProductIfOwner(id, authenticatedUserId);
        productRepository.delete(product);
    }

    @Override
    public List<DtoProduct> findAllByStoreId(Long storeId) {
        List<Product> productList = productRepository.findAllByStoreId(storeId);

        if (productList.isEmpty()) {
            return new ArrayList<>();
        }

        return productList.stream()
                .map(this::dtoTransformation)
                .toList();
    }
}
