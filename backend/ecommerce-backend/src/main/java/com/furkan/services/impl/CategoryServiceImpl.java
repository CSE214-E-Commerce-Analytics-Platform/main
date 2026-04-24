package com.furkan.services.impl;

import com.furkan.dto.request.DtoCategoryRequest;
import com.furkan.dto.response.DtoCategory;
import com.furkan.entities.Category;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.CategoryRepository;
import com.furkan.services.ICategoryService;
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
public class CategoryServiceImpl implements ICategoryService {

    private final CategoryRepository categoryRepository;

    private DtoCategory dtoTransformation(Category category) {
        DtoCategory dto = new DtoCategory();
        BeanUtils.copyProperties(category, dto);
        if (category.getParent() != null) {
            dto.setParentId(category.getParent().getId());
        }
        return dto;
    }

    @Override
    @Transactional
    public DtoCategory createCategory(DtoCategoryRequest input) {
        Category category = new Category();
        category.setName(input.getName());

        if (input.getParentId() != null) {
            Category parent = categoryRepository.findById(input.getParentId())
                    .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.PARENT_NOT_FOUND, input.getParentId().toString())));
            category.setParent(parent);
        }

        return dtoTransformation(categoryRepository.save(category));
    }

    @Override
    public DtoCategory findCategoryById(Long id) {
        Category category = categoryRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.CATEGORY_NOT_FOUND, id.toString())));
        return dtoTransformation(category);
    }

    @Override
    public RestPageableEntity<DtoCategory> findAllCategories(RestPageableRequest request) {
        if (request.getColumnName() == null || request.getColumnName().isEmpty()) {
            request.setColumnName("id");
            request.setAsc(false);
        }

        Pageable pageable = PagerUtil.toPageable(request);

        Page<Category> categoryPage = categoryRepository.findAll(pageable);

        List<DtoCategory> dtoList = categoryPage.getContent().stream()
                .map(this::dtoTransformation)
                .toList();

        return PagerUtil.toPageableResponse(categoryPage, dtoList);
    }

    @Override
    @Transactional
    public DtoCategory updateCategoryById(Long id, DtoCategoryRequest input) {
        Category category = categoryRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.CATEGORY_NOT_FOUND, id.toString())));

        category.setName(input.getName());
        if (input.getParentId() != null) {
            Category parent = categoryRepository.findById(input.getParentId())
                    .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.PARENT_NOT_FOUND, input.getParentId().toString())));
            category.setParent(parent);
        } else {
            category.setParent(null);
        }

        return dtoTransformation(categoryRepository.save(category));
    }

    @Override
    @Transactional
    public void deleteCategoryById(Long id) {
        Category category = categoryRepository.findById(id)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.CATEGORY_NOT_FOUND, id.toString())));

        if (categoryRepository.existsByParentId(id)) {
            throw new BaseException(new ErrorMessage(MessageType.CHILD_CAT_EXISTS, id.toString()));
        }

        categoryRepository.delete(category);
    }
}
