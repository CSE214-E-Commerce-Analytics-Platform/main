import { Component, inject, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ShipmentService } from '../../../core/services/shipment.service';
import { OrderService } from '../../../core/services/order.service';
import { StoreService } from '../../../core/services/store.service';
import { ToastService } from '../../../core/services/toast.service';
import { DtoShipment, DtoShipmentRequest, ShipmentStatus } from '../../../shared/models/shipment';
import { DtoOrder, OrderStatus } from '../../../shared/models/order';
import { Store } from '../../../shared/models/store';
import { catchError } from 'rxjs/operators';
import { of } from 'rxjs';

@Component({
  selector: 'app-corp-shipments',
  imports: [CommonModule, FormsModule],
  templateUrl: './corp-shipments.component.html',
  styleUrl: './corp-shipments.component.css'
})
export class CorpShipmentsComponent implements OnInit {
  private shipmentService = inject(ShipmentService);
  private orderService = inject(OrderService);
  private storeService = inject(StoreService);
  private toastService = inject(ToastService);

  ShipmentStatus = ShipmentStatus;
  OrderStatus = OrderStatus;

  stores: Store[] = [];
  selectedStoreId: number | null = null;
  shipments: DtoShipment[] = [];
  paidOrders: DtoOrder[] = [];

  isLoadingShipments = false;
  isLoadingOrders = false;
  expandedShipmentId: number | null = null;
  showInitForm = false;

  initForm: DtoShipmentRequest = { childOrderId: 0, warehouse: '', mode: '' };
  isInitializing = false;
  updatingId: number | null = null;
  cancellingId: number | null = null;

  ngOnInit(): void {
    this.loadStores();
  }

  loadStores(): void {
    this.storeService.getMyStores().subscribe({
      next: stores => {
        this.stores = stores || [];
        if (this.stores.length > 0 && this.stores[0].id) {
          this.selectedStoreId = this.stores[0].id;
          this.loadShipments();
          this.loadPaidOrders();
        }
      }
    });
  }

  onStoreChange(): void {
    this.shipments = [];
    this.paidOrders = [];
    this.loadShipments();
    this.loadPaidOrders();
  }

  loadShipments(): void {
    this.isLoadingShipments = true;
    this.shipmentService.getMyShipments().pipe(
      catchError(() => {
        this.toastService.showError('Failed to load shipments.');
        this.isLoadingShipments = false;
        return of([]);
      })
    ).subscribe(shipments => {
      this.shipments = shipments || [];
      this.isLoadingShipments = false;
    });
  }

  loadPaidOrders(): void {
    if (!this.selectedStoreId) return;
    this.isLoadingOrders = true;
    this.orderService.getStoreOrders(this.selectedStoreId).pipe(
      catchError(() => {
        this.isLoadingOrders = false;
        return of([]);
      })
    ).subscribe(orders => {
      this.paidOrders = (orders || []).filter(o => o.status === OrderStatus.PAID);
      this.isLoadingOrders = false;
    });
  }

  toggleExpand(id: number | undefined): void {
    if (!id) return;
    this.expandedShipmentId = this.expandedShipmentId === id ? null : id;
  }

  initializeShipment(): void {
    if (!this.initForm.childOrderId || !this.initForm.warehouse || !this.initForm.mode) {
      this.toastService.showError('Please fill in all fields.');
      return;
    }
    this.isInitializing = true;
    this.shipmentService.initialize(this.initForm).pipe(
      catchError(err => {
        this.toastService.showError('Failed to initialize shipment. ' + (err.error?.exception?.message || ''));
        this.isInitializing = false;
        return of(null);
      })
    ).subscribe(shipment => {
      if (shipment) {
        this.toastService.showSuccess('Shipment created! Tracking #: ' + shipment.trackingNumber);
        this.showInitForm = false;
        this.initForm = { childOrderId: 0, warehouse: '', mode: '' };
        this.loadShipments();
        this.loadPaidOrders();
      }
      this.isInitializing = false;
    });
  }

  updateStatus(shipmentId: number, newStatus: ShipmentStatus): void {
    this.updatingId = shipmentId;
    this.shipmentService.updateStatus(shipmentId, newStatus).pipe(
      catchError(err => {
        this.toastService.showError('Failed to update status. ' + (err.error?.exception?.message || ''));
        this.updatingId = null;
        return of(null);
      })
    ).subscribe(updated => {
      if (updated) {
        this.toastService.showSuccess('Status updated to ' + newStatus + '.');
        this.loadShipments();
      }
      this.updatingId = null;
    });
  }

  cancelShipment(shipmentId: number): void {
    this.cancellingId = shipmentId;
    this.shipmentService.cancel(shipmentId).pipe(
      catchError(err => {
        this.toastService.showError('Failed to cancel shipment. ' + (err.error?.exception?.message || ''));
        this.cancellingId = null;
        return of(null);
      })
    ).subscribe(updated => {
      if (updated) {
        this.toastService.showSuccess('Shipment cancelled.');
        this.loadShipments();
      }
      this.cancellingId = null;
    });
  }

  getNextStatuses(current: ShipmentStatus): ShipmentStatus[] {
    switch (current) {
      case ShipmentStatus.PENDING:          return [ShipmentStatus.LABEL_CREATED, ShipmentStatus.IN_TRANSIT];
      case ShipmentStatus.LABEL_CREATED:    return [ShipmentStatus.IN_TRANSIT];
      case ShipmentStatus.IN_TRANSIT:       return [ShipmentStatus.OUT_FOR_DELIVERY];
      case ShipmentStatus.OUT_FOR_DELIVERY: return [ShipmentStatus.DELIVERED, ShipmentStatus.RETURNED];
      default:                               return [];
    }
  }

  canCancel(status: ShipmentStatus): boolean {
    return status === ShipmentStatus.PENDING || status === ShipmentStatus.LABEL_CREATED;
  }

  getStatusClass(status: ShipmentStatus): string {
    switch (status) {
      case ShipmentStatus.PENDING:          return 'status-pending';
      case ShipmentStatus.LABEL_CREATED:    return 'status-label';
      case ShipmentStatus.IN_TRANSIT:       return 'status-transit';
      case ShipmentStatus.OUT_FOR_DELIVERY: return 'status-out';
      case ShipmentStatus.DELIVERED:        return 'status-delivered';
      case ShipmentStatus.RETURNED:         return 'status-returned';
      case ShipmentStatus.CANCELLED:        return 'status-cancelled';
      default:                               return '';
    }
  }

  getStatusIcon(status: ShipmentStatus): string {
    switch (status) {
      case ShipmentStatus.PENDING:          return '⏳';
      case ShipmentStatus.LABEL_CREATED:    return '🏷️';
      case ShipmentStatus.IN_TRANSIT:       return '🚚';
      case ShipmentStatus.OUT_FOR_DELIVERY: return '🏃';
      case ShipmentStatus.DELIVERED:        return '✅';
      case ShipmentStatus.RETURNED:         return '↩️';
      case ShipmentStatus.CANCELLED:        return '❌';
      default:                               return '•';
    }
  }
}
