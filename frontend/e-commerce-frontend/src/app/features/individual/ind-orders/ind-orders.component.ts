import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { OrderService } from '../../../core/services/order.service';
import { PaymentService } from '../../../core/services/payment.service';
import { ToastService } from '../../../core/services/toast.service';
import { DtoOrder, OrderStatus } from '../../../shared/models/order';
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';

@Component({
  selector: 'app-ind-orders',
  imports: [CommonModule],
  templateUrl: './ind-orders.component.html',
  styleUrl: './ind-orders.component.css'
})
export class IndOrdersComponent implements OnInit {
  private orderService = inject(OrderService);
  private paymentService = inject(PaymentService);
  private toastService = inject(ToastService);

  orders: DtoOrder[] = [];
  isLoading = true;
  cancellingId: number | null = null;
  payingId: number | null = null;

  expandedOrderId: number | null = null;

  ngOnInit(): void {
    this.loadOrders();
  }

  loadOrders(): void {
    this.isLoading = true;
    this.orderService.getMyOrders().pipe(
      catchError(() => {
        this.toastService.showError('Failed to load orders.');
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

  cancelOrder(orderId: number | undefined): void {
    if (!orderId) return;
    this.cancellingId = orderId;
    this.orderService.cancel(orderId).pipe(
      catchError(err => {
        this.toastService.showError('Failed to cancel order. ' + (err.error?.exception?.message || ''));
        this.cancellingId = null;
        return of(null);
      })
    ).subscribe(res => {
      if (res !== null) {
        this.toastService.showSuccess('Order cancelled successfully.');
        this.loadOrders();
      }
      this.cancellingId = null;
    });
  }

  payNow(orderId: number | undefined): void {
    if (!orderId) return;
    this.payingId = orderId;
    this.paymentService.create({ orderId }).pipe(
      catchError(err => {
        this.toastService.showError('Payment initiation failed. ' + (err.error?.exception?.message || ''));
        this.payingId = null;
        return of(null);
      })
    ).subscribe(payment => {
      if (payment && payment.transactionKey) {
        this.toastService.showSuccess('Redirecting to payment...');
        window.location.href = payment.transactionKey;
      } else {
        this.payingId = null;
      }
    });
  }

  isCancellable(order: DtoOrder): boolean {
    return order.status === OrderStatus.PENDING || order.status === OrderStatus.APPROVED;
  }

  isPending(order: DtoOrder): boolean {
    return order.status === OrderStatus.PENDING;
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
