package com.furkan.controllers;

import com.furkan.utils.RootEntity;
import org.springframework.security.core.Authentication;

public interface IRestAiController {

    RootEntity<String> askAi(String question, Authentication authentication);
}
