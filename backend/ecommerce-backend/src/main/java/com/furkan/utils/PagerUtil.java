package com.furkan.utils;

import lombok.experimental.UtilityClass;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.List;

@UtilityClass
public class PagerUtil {

    public boolean isNullOrEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

    public Pageable toPageable(RestPageableRequest request) {
        if (!isNullOrEmpty(request.getColumnName())) {
            Sort sortBy = request.isAsc() ? Sort.by(Sort.Direction.ASC, request.getColumnName())
                    : Sort.by(Sort.Direction.DESC, request.getColumnName());

            return PageRequest.of(request.getPageNumber(), request.getPageSize(), sortBy);
        }
        return PageRequest.of(request.getPageNumber(), request.getPageSize());
    }

    public <T> RestPageableEntity<T> toPageableResponse(Page<?> page, List<T> content) {
        RestPageableEntity<T> pageableEntity = new RestPageableEntity<>();
        pageableEntity.setContent(content);
        pageableEntity.setPageNumber(page.getPageable().getPageNumber());
        pageableEntity.setPageSize(page.getPageable().getPageSize());
        pageableEntity.setTotalElement(page.getTotalElements());
        return pageableEntity;
    }
}
