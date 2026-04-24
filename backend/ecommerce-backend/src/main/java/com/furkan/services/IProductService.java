package com.furkan.services;

import com.furkan.dto.request.DtoProductRequest;
import com.furkan.dto.response.DtoProduct;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;

public interface IProductService {

    DtoProduct createProduct(DtoProductRequest input, Long authenticatedUserId);

    DtoProduct findProductById(Long id);

    RestPageableEntity<DtoProduct> findAllProducts(RestPageableRequest request);

    DtoProduct updateProductById(Long id, DtoProductRequest input, Long authenticatedUserId);

    void deleteProductById(Long id, Long authenticatedUserId);

    RestPageableEntity<DtoProduct> findAllByStoreId(Long storeId, RestPageableRequest request);

    void reduceStock(Long productId, int quantity);

    void increaseStock(Long productId, int quantity);
}
