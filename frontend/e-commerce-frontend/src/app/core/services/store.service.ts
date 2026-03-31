import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';
import { Store, StoreRequest } from '../../shared/models/store';
import { ApiResponse } from '../../shared/models/api-response';
import { environment } from '../../../environments/environment.development';

@Injectable({
    providedIn: 'root'
})
export class StoreService {

    private readonly apiUrl = `${environment.baseUrl}/stores`;

    constructor(private http: HttpClient) { }

    createStore(store: StoreRequest): Observable<Store> {
        return this.http.post<ApiResponse<Store>>(this.apiUrl, store).pipe(
            map(res => res.payload as Store)
        );
    }

    getMyStores(): Observable<Store[]> {
        return this.http.get<ApiResponse<Store[]>>(`${this.apiUrl}/my-stores`).pipe(
            map(res => res.payload as Store[])
        );
    }

    getStoreById(id: number): Observable<Store> {
        return this.http.get<ApiResponse<Store>>(`${this.apiUrl}/${id}`).pipe(
            map(res => res.payload as Store)
        );
    }

    getAllStores(): Observable<Store[]> {
        return this.http.get<ApiResponse<Store[]>>(this.apiUrl).pipe(
            map(res => res.payload as Store[])
        );
    }

    updateStore(id: number, store: StoreRequest): Observable<Store> {
        return this.http.put<ApiResponse<Store>>(`${this.apiUrl}/${id}`, store).pipe(
            map(res => res.payload as Store)
        );
    }

    deleteStore(id: number): Observable<void> {
        return this.http.delete<ApiResponse<void>>(`${this.apiUrl}/${id}`).pipe(
            map(() => void 0)
        );
    }
}
