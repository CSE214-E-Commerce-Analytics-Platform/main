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
    public DtoProduct createProduct(DtoProductRequest input) {
        Product product = new Product();
        BeanUtils.copyProperties(input, product);

        Store store = storeRepository.findById(input.getStoreId())
                .orElseThrow(() -> new RuntimeException("Store not found!"));
        product.setStore(store);

        if (input.getCategoryId() != null) {
            Category category = categoryRepository.findById(input.getCategoryId())
                    .orElseThrow(() -> new RuntimeException("Category not found!"));
            product.setCategory(category);
        }

        return dtoTransformation(productRepository.save(product));
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
    public DtoProduct updateProductById(Long id, DtoProductRequest input) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found!"));

        product.setName(input.getName());
        product.setDescription(input.getDescription());
        product.setUnitPrice(input.getUnitPrice());
        product.setStockQuantity(input.getStockQuantity());
        product.setImageUrl(input.getImageUrl());

        return dtoTransformation(productRepository.save(product));
    }

    @Override
    @Transactional
    public void deleteProductById(Long id) {
        Product product = productRepository.findById(id)
                        .orElseThrow(() -> new RuntimeException("Product not found!"));
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
