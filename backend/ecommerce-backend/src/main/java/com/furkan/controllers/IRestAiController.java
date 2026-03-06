package com.furkan.controllers;

import com.furkan.utils.RootEntity;

public interface IRestAiController {

    RootEntity<String> askAi(String question, Long storeId);
}
