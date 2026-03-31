package com.furkan.services.impl;

import com.furkan.dto.request.DtoCategoryRequest;
import com.furkan.dto.response.DtoCategory;
import com.furkan.entities.Category;
import com.furkan.repositories.CategoryRepository;
import com.furkan.services.ICategoryService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
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
                    .orElseThrow(() -> new RuntimeException("Parent category not found!"));
            category.setParent(parent);
        }

        return dtoTransformation(categoryRepository.save(category));
    }

    @Override
    public DtoCategory findCategoryById(Long id) {
        Category category = categoryRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Category not found!"));
        return dtoTransformation(category);
    }

    @Override
    public List<DtoCategory> findAllCategories() {
        return categoryRepository.findAll().stream()
                .map(this::dtoTransformation)
                .toList();
    }

    @Override
    @Transactional
    public DtoCategory updateCategoryById(Long id, DtoCategoryRequest input) {
        Category category = categoryRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Category not found!"));

        category.setName(input.getName());
        if (input.getParentId() != null) {
            Category parent = categoryRepository.findById(input.getParentId())
                    .orElseThrow(() -> new RuntimeException("Parent category not found!"));
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
                .orElseThrow(() -> new RuntimeException("Category not found!"));

        if (categoryRepository.existsByParentId(category.getParent().getId())) {
            throw new RuntimeException("This category has children categories, first delete these!");
        }

        categoryRepository.delete(category);
    }
}
