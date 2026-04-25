package com.furkan.services.impl;

import com.furkan.dto.request.DtoProductRequest;
import com.furkan.dto.response.DtoProduct;
import com.furkan.entities.Category;
import com.furkan.entities.Product;
import com.furkan.entities.Store;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.CategoryRepository;
import com.furkan.repositories.ProductRepository;
import com.furkan.repositories.StoreRepository;
import com.furkan.services.IProductService;
import com.furkan.utils.PagerUtil;
import com.furkan.utils.RestPageableEntity;
import com.furkan.utils.RestPageableRequest;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

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
            throw new BaseException(new ErrorMessage(MessageType.SKU_ALREADY_EXISTS, input.getSku()));
        }

        Store store = storeRepository.findById(input.getStoreId())
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.STORE_NOT_FOUND, input.getStoreId().toString())));

        Product product = new Product();
        BeanUtils.copyProperties(input, product);
        product.setStore(store);

        if (input.getCategoryId() != null) {
            Category category = categoryRepository.findById(input.getCategoryId())
                    .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.CATEGORY_NOT_FOUND, input.getCategoryId().toString())));
            product.setCategory(category);
        }

        return dtoTransformation(productRepository.save(product));
    }

    private Product getProductIfOwner(Long productId, Long userId) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.PRODUCT_NOT_FOUND, productId.toString())));

        if (!product.getStore().getOwner().getId().equals(userId)) {
            throw new BaseException(new ErrorMessage(MessageType.UNAUTHORIZED_TRANSACTION, productId.toString()));
        }

        return product;
    }

    @Override
    public DtoProduct findProductById(Long id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.PRODUCT_NOT_FOUND, id.toString())));
        return dtoTransformation(product);
    }

    @Override
    public RestPageableEntity<DtoProduct> findAllProducts(RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Product> productPage = productRepository.findAll(pageable);

        List<DtoProduct> dtoList = productPage.getContent().stream()
                .map(this::dtoTransformation)
                .toList();

        return PagerUtil.toPageableResponse(productPage, dtoList);
    }

    @Override
    @Transactional
    public DtoProduct updateProductById(Long id, DtoProductRequest input, Long authenticatedUserId) {
        Product product = getProductIfOwner(id, authenticatedUserId);

        if (!product.getSku().equals(input.getSku()) && productRepository.existsBySku(input.getSku())) {
            throw new BaseException(new ErrorMessage(MessageType.SKU_ALREADY_EXISTS, input.getSku()));
        }

        product.setName(input.getName());
        product.setDescription(input.getDescription());
        product.setUnitPrice(input.getUnitPrice());
        product.setStockQuantity(input.getStockQuantity());
        product.setImageUrl(input.getImageUrl());
        product.setSku(input.getSku());

        if (input.getCategoryId() != null && (product.getCategory() == null || !product.getCategory().getId().equals(input.getCategoryId()))) {
            Category category = categoryRepository.findById(input.getCategoryId())
                    .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.CATEGORY_NOT_FOUND, input.getCategoryId().toString())));
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
    public RestPageableEntity<DtoProduct> findAllByStoreId(Long storeId, RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Product> productPage = productRepository.findAllByStoreId(storeId, pageable);

        List<DtoProduct> dtoList = productPage.getContent().stream()
                .map(this::dtoTransformation)
                .toList();

        return PagerUtil.toPageableResponse(productPage, dtoList);
    }

    @Override
    @Transactional
    public void reduceStock(Long productId, int quantity) {
        Product product = productRepository.findById(productId).orElseThrow();
        if (product.getStockQuantity() < quantity) {
            throw new BaseException(new ErrorMessage(MessageType.INSUFFICIENT_STOCK, product.getName()));
        }
        product.setStockQuantity(product.getStockQuantity() - quantity);
        productRepository.save(product);
    }

    @Override
    @Transactional
    public void increaseStock(Long productId, int quantity) {
        Product product = productRepository.findById(productId).orElseThrow();

        product.setStockQuantity(product.getStockQuantity() + quantity);
        productRepository.save(product);
    }
}
