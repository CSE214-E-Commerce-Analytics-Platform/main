package com.furkan.services.impl;

import com.furkan.dto.response.DtoCustomerAnalytics;
import com.furkan.dto.response.DtoCustomerSegment;
import com.furkan.dto.response.DtoTopCustomer;
import com.furkan.entities.Order;
import com.furkan.entities.User;
import com.furkan.enums.OrderStatus;
import com.furkan.exception.BaseException;
import com.furkan.exception.ErrorMessage;
import com.furkan.exception.MessageType;
import com.furkan.repositories.OrderRepository;
import com.furkan.repositories.UserRepository;
import com.furkan.services.IAnalyticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class AnalyticsServiceImpl implements IAnalyticsService {

    private final OrderRepository orderRepository;
    private final UserRepository userRepository;

    @Override
    public DtoCustomerAnalytics getStoreCustomerAnalytics(Long storeId, Long authenticatedUserId) {
        User currentUser = userRepository.findById(authenticatedUserId)
                .orElseThrow(() -> new BaseException(new ErrorMessage(MessageType.USER_NOT_FOUND, authenticatedUserId.toString())));

        if (!currentUser.getRoleType().name().equals("ADMIN") &&
                (currentUser.getStore() == null || !currentUser.getStore().getId().equals(storeId))) {
            throw new BaseException(new ErrorMessage(MessageType.UNAUTHORIZED, "You can only view analytics for your own store."));
        }

        List<Order> storeOrders = orderRepository.findByStoreIdOrderByOrderDateDesc(storeId).stream()
                .filter(o -> o.getStatus() != OrderStatus.CANCELLED)
                .toList();

        Map<User, List<Order>> userOrdersMap = storeOrders.stream()
                .collect(Collectors.groupingBy(Order::getUser));

        int newCount = 0;
        int returningCount = 0;
        int vipCount = 0;
        List<DtoTopCustomer> topCustomersList = new ArrayList<>();

        for (Map.Entry<User, List<Order>> entry : userOrdersMap.entrySet()) {
            User customer = entry.getKey();
            List<Order> customerOrders = entry.getValue();

            int orderCount = customerOrders.size();

            if (orderCount == 1) newCount++;
            else if (orderCount >= 2 && orderCount <= 4) returningCount++;
            else vipCount++;

            BigDecimal totalSpend = customerOrders.stream()
                    .map(Order::getGrandTotal)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);

            DtoTopCustomer topCustomer = new DtoTopCustomer();
            topCustomer.setUserId(customer.getId());
            topCustomer.setEmail(customer.getEmail());
            topCustomer.setTotalOrders(orderCount);
            topCustomer.setTotalSpend(totalSpend);

            topCustomersList.add(topCustomer);
        }

        List<DtoTopCustomer> top5Customers = topCustomersList.stream()
                .sorted(Comparator.comparing(DtoTopCustomer::getTotalSpend).reversed())
                .limit(5)
                .collect(Collectors.toList());

        List<DtoCustomerSegment> segments = List.of(
                new DtoCustomerSegment("New", newCount),
                new DtoCustomerSegment("Returning", returningCount),
                new DtoCustomerSegment("Loyal", vipCount)
        );

        DtoCustomerAnalytics response = new DtoCustomerAnalytics();
        response.setTotalCustomers(userOrdersMap.size());
        response.setSegments(segments);
        response.setTopCustomers(top5Customers);


        return response;
    }
}
