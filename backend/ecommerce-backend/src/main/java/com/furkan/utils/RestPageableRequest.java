package com.furkan.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RestPageableRequest {

    private int pageNumber;

    private int pageSize;

    private String columnName;

    private boolean asc;
}
