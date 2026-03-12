package com.furkan.scheduler;

import com.furkan.repositories.RefreshTokenRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;


@Component
@RequiredArgsConstructor
@Slf4j
public class TokenCleanupTask {

    private final RefreshTokenRepository refreshTokenRepository;

    // Her gece 02:00'da çalışır
    @Scheduled(cron = "0 0 2 * * *")
    @Transactional
    public void cleanExpiredTokens() {
        int deleted = refreshTokenRepository.deleteAllExpiredOrRevoked(LocalDateTime.now());
        if (deleted > 0) {
            log.info("[Token Cleanup] {} expired/revoked refresh token(s) deleted.", deleted);
        }
    }
}
