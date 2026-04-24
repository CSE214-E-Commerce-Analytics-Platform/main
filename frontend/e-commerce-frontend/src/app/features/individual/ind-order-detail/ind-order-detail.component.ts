import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { OrderService } from '../../../core/services/order.service';
import { ShipmentService } from '../../../core/services/shipment.service';
import { ReviewService } from '../../../core/services/review.service';
import { ToastService } from '../../../core/services/toast.service';
import { DtoOrder, OrderStatus } from '../../../shared/models/order';
import { DtoShipment, ShipmentStatus } from '../../../shared/models/shipment';
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';
import { ReviewWidgetComponent } from '../../../shared/components/review-widget/review-widget.component';

@Component({
  selector: 'app-ind-order-detail',
  standalone: true,
  imports: [CommonModule, RouterLink, ReviewWidgetComponent],
  templateUrl: './ind-order-detail.component.html',
  styleUrl: './ind-order-detail.component.css'
})
export class IndOrderDetailComponent implements OnInit {
  private route = inject(ActivatedRoute);
  private orderService = inject(OrderService);
  private shipmentService = inject(ShipmentService);
  private reviewService = inject(ReviewService);
  private toastService = inject(ToastService);

  order: DtoOrder | null = null;
  isLoading = true;
  shipmentsMap: Record<number, DtoShipment | null> = {};
  
  // Review State
  reviewedProductIds = new Set<number>();
  reviewProductId: number | null = null;
  showReviewModal = false;

  ngOnInit(): void {
    const idParam = this.route.snapshot.paramMap.get('id');
    if (idParam) {
      this.loadOrder(parseInt(idParam, 10));
      this.loadMyReviews();
    } else {
      this.toastService.showError('Invalid order ID.');
      this.isLoading = false;
    }
  }

  loadMyReviews(): void {
    this.reviewService.getMyReviews({ pageNumber: 0, pageSize: 100 }).pipe(
      catchError(() => of(null))
    ).subscribe(res => {
      (res?.content || []).forEach(r => {
        if (r.productId) {
          this.reviewedProductIds.add(r.productId);
        }
      });
    });
  }

  loadOrder(orderId: number): void {
    this.isLoading = true;
    this.orderService.getById(orderId).pipe(
      catchError(() => {
        this.toastService.showError('Failed to load order details.');
        this.isLoading = false;
        return of(null);
      })
    ).subscribe(order => {
      this.order = order;
      if (order && order.subOrders) {
        order.subOrders.forEach(sub => {
          if (sub.id) {
            this.loadShipmentForSubOrder(sub.id);
          }
        });
      }
      this.isLoading = false;
    });
  }

  loadShipmentForSubOrder(subOrderId: number): void {
    this.shipmentsMap[subOrderId] = null;
    this.shipmentService.getByOrderId(subOrderId).pipe(
      catchError(() => of(null))
    ).subscribe(shipment => {
      this.shipmentsMap[subOrderId] = shipment;
    });
  }

  getStatusClass(status: OrderStatus | ShipmentStatus): string {
    switch (status) {
      case OrderStatus.PENDING: return 'status-pending';
      case OrderStatus.PAID: return 'status-approved';
      case OrderStatus.PARTIALLY_SHIPPED: return 'status-shipped';
      case OrderStatus.SHIPPED: return 'status-shipped';
      case OrderStatus.DELIVERED: return 'status-delivered';
      case OrderStatus.CANCELLED: return 'status-cancelled';
      case ShipmentStatus.PENDING: return 'status-pending';
      case ShipmentStatus.LABEL_CREATED: return 'status-approved';
      case ShipmentStatus.IN_TRANSIT: return 'status-shipped';
      case ShipmentStatus.OUT_FOR_DELIVERY: return 'status-shipped';
      case ShipmentStatus.DELIVERED: return 'status-delivered';
      case ShipmentStatus.RETURNED: return 'status-cancelled';
      case ShipmentStatus.CANCELLED: return 'status-cancelled';
      default: return '';
    }
  }

  isSubOrderDelivered(subOrder: DtoOrder): boolean {
    if (subOrder.status === OrderStatus.DELIVERED) return true;
    if (subOrder.id && this.shipmentsMap[subOrder.id]) {
      return this.shipmentsMap[subOrder.id]?.status === ShipmentStatus.DELIVERED;
    }
    return false;
  }

  openReviewModal(productId: number, event: Event): void {
    event.stopPropagation();
    this.reviewProductId = productId;
    this.showReviewModal = true;
  }

  closeReviewModal(): void {
    this.showReviewModal = false;
    this.reviewProductId = null;
  }
}
