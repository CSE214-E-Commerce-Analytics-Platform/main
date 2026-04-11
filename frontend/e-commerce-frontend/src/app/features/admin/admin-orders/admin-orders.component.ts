import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { OrderService } from '../../../core/services/order.service';
import { ToastService } from '../../../core/services/toast.service';
import { DtoOrder, OrderStatus } from '../../../shared/models/order';
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';

@Component({
  selector: 'app-admin-orders',
  imports: [CommonModule],
  templateUrl: './admin-orders.component.html',
  styleUrl: './admin-orders.component.css'
})
export class AdminOrdersComponent implements OnInit {
  private orderService = inject(OrderService);
  private toastService = inject(ToastService);

  orders: DtoOrder[] = [];
  isLoading = true;
  expandedOrderId: number | null = null;

  ngOnInit(): void {
    this.loadAllOrders();
  }

  loadAllOrders(): void {
    this.isLoading = true;
    this.orderService.getAllOrders().pipe(
      catchError(() => {
        this.toastService.showError('Failed to load global orders.');
        this.isLoading = false;
        return of([]);
      })
    ).subscribe(orders => {
      this.orders = orders;
      this.isLoading = false;
    });
  }

  toggleExpand(orderId: number | undefined): void {
    if (!orderId) return;
    this.expandedOrderId = this.expandedOrderId === orderId ? null : orderId;
  }

  getStatusClass(status: OrderStatus): string {
    switch (status) {
      case OrderStatus.PENDING:   return 'status-pending';
      case OrderStatus.APPROVED:  return 'status-approved';
      case OrderStatus.SHIPPED:   return 'status-shipped';
      case OrderStatus.DELIVERED: return 'status-delivered';
      case OrderStatus.CANCELLED: return 'status-cancelled';
      default: return '';
    }
  }

  getStatusIcon(status: OrderStatus): string {
    switch (status) {
      case OrderStatus.PENDING:   return '⏳';
      case OrderStatus.APPROVED:  return '✅';
      case OrderStatus.SHIPPED:   return '🚚';
      case OrderStatus.DELIVERED: return '📦';
      case OrderStatus.CANCELLED: return '❌';
      default: return '•';
    }
  }
}
