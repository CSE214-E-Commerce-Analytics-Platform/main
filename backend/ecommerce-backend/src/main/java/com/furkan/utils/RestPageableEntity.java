package com.furkan.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RestPageableEntity<T> {

    private List<T> content;

    private int pageNumber;

    private int pageSize;

    private long totalElement;
}
