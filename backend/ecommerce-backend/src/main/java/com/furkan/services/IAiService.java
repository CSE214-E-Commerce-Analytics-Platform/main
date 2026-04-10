package com.furkan.services;

import org.springframework.security.core.Authentication;

public interface IAiService {

    String askAi(String userQuestion, Authentication authentication);
}
