import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { OrderService } from '../../../core/services/order.service';
import { StoreService } from '../../../core/services/store.service';
import { ToastService } from '../../../core/services/toast.service';
import { DtoOrder, OrderStatus } from '../../../shared/models/order';
import { Store } from '../../../shared/models/store';
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';

@Component({
  selector: 'app-corp-orders',
  imports: [CommonModule, FormsModule],
  templateUrl: './corp-orders.component.html',
  styleUrl: './corp-orders.component.css'
})
export class CorpOrdersComponent implements OnInit {
  private orderService = inject(OrderService);
  private storeService = inject(StoreService);
  private toastService = inject(ToastService);

  OrderStatus = OrderStatus;

  stores: Store[] = [];
  selectedStoreId: number | null = null;
  orders: DtoOrder[] = [];
  isLoading = false;
  expandedOrderId: number | null = null;



  ngOnInit(): void {
    this.storeService.getMyStores().subscribe({
      next: (stores) => {
        this.stores = stores || [];
        if (this.stores.length > 0 && this.stores[0].id) {
          this.selectedStoreId = this.stores[0].id;
          this.loadOrders();
        }
      }
    });
  }

  onStoreChange(): void {
    this.orders = [];
    this.loadOrders();
  }

  loadOrders(): void {
    if (!this.selectedStoreId) return;
    this.isLoading = true;
    this.orderService.getStoreOrders(this.selectedStoreId).pipe(
      catchError(() => {
        this.toastService.showError('Failed to load store orders.');
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

  updateStatus(order: DtoOrder, newStatus: OrderStatus): void {
    if (!order.id || !this.selectedStoreId) return;
    this.orderService.updateSubOrderStatus(order.id, newStatus, this.selectedStoreId).pipe(
      catchError(err => {
        this.toastService.showError('Failed to update status. ' + (err.error?.exception?.message || ''));
        return of(null);
      })
    ).subscribe(updated => {
      if (updated) {
        this.toastService.showSuccess(`Order status updated to ${newStatus}.`);
        this.loadOrders();
      }
    });
  }

  getStatusClass(status: OrderStatus): string {
    switch (status) {
      case OrderStatus.PENDING:   return 'status-pending';
      case OrderStatus.PAID:  return 'status-approved';
      case OrderStatus.PARTIALLY_SHIPPED:   return 'status-shipped';
      case OrderStatus.SHIPPED:   return 'status-shipped';
      case OrderStatus.DELIVERED: return 'status-delivered';
      case OrderStatus.CANCELLED: return 'status-cancelled';
      default: return '';
    }
  }

  getStatusIcon(status: OrderStatus): string {
    switch (status) {
      case OrderStatus.PENDING:   return '⏳';
      case OrderStatus.PAID:  return '✅';
      case OrderStatus.PARTIALLY_SHIPPED:   return '📦';
      case OrderStatus.SHIPPED:   return '🚚';
      case OrderStatus.DELIVERED: return '🎉';
      case OrderStatus.CANCELLED: return '❌';
      default: return '•';
    }
  }
}
