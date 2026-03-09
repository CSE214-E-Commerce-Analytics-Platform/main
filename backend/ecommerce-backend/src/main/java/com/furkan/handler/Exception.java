package com.furkan.handler;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Exception<E> {

    private String path;

    private LocalDateTime createTime;

    private String hostName;

    private E message;
}
