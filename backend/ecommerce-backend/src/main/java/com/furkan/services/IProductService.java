package com.furkan.services;

import com.furkan.dto.request.DtoProductRequest;
import com.furkan.dto.response.DtoProduct;

import java.util.List;

public interface IProductService {

    DtoProduct createProduct(DtoProductRequest input, Long authenticatedUserId);

    DtoProduct findProductById(Long id);

    List<DtoProduct> findAllProducts();

    DtoProduct updateProductById(Long id, DtoProductRequest input, Long authenticatedUserId);

    void deleteProductById(Long id, Long authenticatedUserId);

    List<DtoProduct> findAllByStoreId(Long storeId);

    void reduceStock(Long productId, int quantity);

    void increaseStock(Long productId, int quantity);
}
