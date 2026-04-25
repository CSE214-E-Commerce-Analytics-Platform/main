package com.furkan.controllers;

import com.furkan.dto.request.DtoProductRequest;
import com.furkan.dto.response.DtoProduct;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import com.furkan.utils.RootEntity;
import org.springframework.security.core.userdetails.UserDetails;

public interface IRestProductController {

    RootEntity<DtoProduct> createProduct(DtoProductRequest input, UserDetails userDetails);

    RootEntity<DtoProduct> findProductById(Long id);

    RootEntity<RestPageableEntity<DtoProduct>> findAllProducts(RestPageableRequest request);

    RootEntity<DtoProduct> updateProductById(Long id, DtoProductRequest input, UserDetails userDetails);

    RootEntity<Void> deleteProductById(Long id, UserDetails userDetails);

    RootEntity<RestPageableEntity<DtoProduct>> findAllByStoreId(Long storeId, RestPageableRequest request);
}
