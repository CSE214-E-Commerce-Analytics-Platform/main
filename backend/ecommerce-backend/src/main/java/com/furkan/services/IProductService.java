package com.furkan.services;

import com.furkan.dto.request.DtoProductRequest;
import com.furkan.dto.response.DtoProduct;

import java.util.List;

public interface IProductService {

    DtoProduct createProduct(DtoProductRequest input);

    DtoProduct findProductById(Long id);

    List<DtoProduct> findAllProducts();

    DtoProduct updateProductById(Long id, DtoProductRequest input);

    void deleteProductById(Long id);

    List<DtoProduct> findAllByStoreId(Long storeId);
}
