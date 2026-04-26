package com.furkan.dto.request;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class DtoAiRequest {

    @NotBlank(message = "Question field can not be empty!")
    String question;

    List<Map<String, String>> conversationHistory = new ArrayList<>();
}
