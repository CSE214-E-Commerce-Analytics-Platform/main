import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';
import { ApiResponse } from '../../shared/models/api-response';
import { DtoOrder, DtoOrderRequest, OrderStatus } from '../../shared/models/order';
import { environment } from '../../../environments/environment';

@Injectable({
  providedIn: 'root'
})
export class OrderService {
  private http = inject(HttpClient);
  private apiUrl = `${environment.baseUrl}/orders`;

  create(request: DtoOrderRequest): Observable<DtoOrder> {
    return this.http.post<ApiResponse<DtoOrder>>(`${this.apiUrl}/create`, request)
      .pipe(map(res => res.payload as DtoOrder));
  }

  getMyOrders(): Observable<DtoOrder[]> {
    return this.http.get<ApiResponse<DtoOrder[]>>(`${this.apiUrl}/my-orders`)
      .pipe(map(res => res.payload as DtoOrder[]));
  }

  cancel(orderId: number): Observable<void> {
    return this.http.put<ApiResponse<void>>(`${this.apiUrl}/${orderId}/cancel`, {})
      .pipe(map(res => res.payload as void));
  }

  getStoreOrders(storeId: number): Observable<DtoOrder[]> {
    return this.http.get<ApiResponse<DtoOrder[]>>(`${this.apiUrl}/store/${storeId}`)
      .pipe(map(res => res.payload as DtoOrder[]));
  }

  updateSubOrderStatus(subOrderId: number, status: OrderStatus, storeId: number): Observable<DtoOrder> {
    return this.http.patch<ApiResponse<DtoOrder>>(`${this.apiUrl}/sub-order/${subOrderId}/status?status=${status}&storeId=${storeId}`, {})
      .pipe(map(res => res.payload as DtoOrder));
  }

  getAllOrders(): Observable<DtoOrder[]> {
    return this.http.get<ApiResponse<DtoOrder[]>>(`${this.apiUrl}/admin/all`)
      .pipe(map(res => res.payload as DtoOrder[]));
  }

  getById(orderId: number): Observable<DtoOrder> {
    return this.http.get<ApiResponse<DtoOrder>>(`${this.apiUrl}/${orderId}`)
      .pipe(map(res => res.payload as DtoOrder));
  }
}
