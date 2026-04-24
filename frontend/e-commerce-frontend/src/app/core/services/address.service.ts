import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';
import { ApiResponse } from '../../shared/models/api-response';
import { DtoAddress, DtoAddressRequest } from '../../shared/models/address';
import { environment } from '../../../environments/environment.development';

@Injectable({
  providedIn: 'root'
})
export class AddressService {
  private http = inject(HttpClient);
  private apiUrl = `${environment.baseUrl}/addresses`;

  createAddress(request: DtoAddressRequest): Observable<DtoAddress> {
    return this.http.post<ApiResponse<DtoAddress>>(this.apiUrl, request)
      .pipe(map(res => res.payload as DtoAddress));
  }

  findMyAddresses(): Observable<DtoAddress[]> {
    return this.http.get<ApiResponse<DtoAddress[]>>(`${this.apiUrl}/my-addresses`)
      .pipe(map(res => res.payload as DtoAddress[]));
  }

  updateAddress(addressId: number, request: DtoAddressRequest): Observable<DtoAddress> {
    return this.http.put<ApiResponse<DtoAddress>>(`${this.apiUrl}/${addressId}`, request)
      .pipe(map(res => res.payload as DtoAddress));
  }

  deleteAddress(addressId: number): Observable<void> {
    return this.http.delete<ApiResponse<void>>(`${this.apiUrl}/${addressId}`)
      .pipe(map(() => void 0));
  }
}
