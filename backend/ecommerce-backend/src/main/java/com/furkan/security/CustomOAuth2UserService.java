package com.furkan.security;

import com.furkan.entities.Cart;
import com.furkan.entities.CustomerProfile;
import com.furkan.entities.User;
import com.furkan.enums.AuthProvider;
import com.furkan.enums.MembershipType;
import com.furkan.enums.RoleType;
import com.furkan.repositories.CartRepository;
import com.furkan.repositories.CustomerProfileRepository;
import com.furkan.repositories.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.client.userinfo.DefaultOAuth2UserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class CustomOAuth2UserService extends DefaultOAuth2UserService {

    private final UserRepository userRepository;
    private final CustomerProfileRepository customerProfileRepository;
    private final CartRepository cartRepository;
    private final PasswordEncoder passwordEncoder;

    @Override
    @Transactional
    public OAuth2User loadUser(OAuth2UserRequest request) throws OAuth2AuthenticationException {
        OAuth2User oAuth2User = super.loadUser(request);

        String registrationId = request.getClientRegistration().getRegistrationId();
        Map<String, Object> attributes = oAuth2User.getAttributes();

        String email = extractEmail(attributes);
        if (email == null || email.isBlank()) {
            throw new OAuth2AuthenticationException(new OAuth2Error("email_not_found"),
                    "Email not available from " + registrationId + ". Please make your email public.");
        }

        AuthProvider provider = AuthProvider.valueOf(registrationId.toUpperCase());

        User user = userRepository.findByEmail(email)
                .map(existing -> {
                    // backfill provider for users created before this column existed
                    if (existing.getProvider() == null) {
                        existing.setProvider(provider);
                        return userRepository.save(existing);
                    }
                    return existing;
                })
                .orElseGet(() -> createUser(email, provider));

        return new OAuth2UserPrincipal(user, attributes);
    }

    private String extractEmail(Map<String, Object> attributes) {
        return (String) attributes.get("email");
    }

    private User createUser(String email, AuthProvider provider) {
        User user = new User();
        user.setEmail(email);
        // Social users get a random unguessable password hash — they cannot log in via the form
        user.setPasswordHash(passwordEncoder.encode(UUID.randomUUID().toString()));
        user.setRoleType(RoleType.INDIVIDUAL);
        user.setProvider(provider);
        user.setActive(true);
        User saved = userRepository.save(user);

        CustomerProfile profile = new CustomerProfile();
        profile.setUser(saved);
        profile.setMembershipType(MembershipType.STANDARD);
        profile.setCreatedAt(LocalDateTime.now());
        profile.setUpdatedAt(LocalDateTime.now());
        customerProfileRepository.save(profile);

        Cart cart = new Cart();
        cart.setUser(saved);
        cart.setTotalPrice(BigDecimal.ZERO);
        cartRepository.save(cart);

        return saved;
    }
}
