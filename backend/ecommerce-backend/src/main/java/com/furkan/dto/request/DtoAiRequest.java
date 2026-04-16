package com.furkan.dto.request;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class DtoAiRequest {

    @NotBlank(message = "Question field can not be empty!")
    String question;
}
