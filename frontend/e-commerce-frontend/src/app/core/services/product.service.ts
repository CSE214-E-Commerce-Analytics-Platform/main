import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';
import { Product } from '../../shared/models/product';
import { ApiResponse } from '../../shared/models/api-response';
import { environment } from '../../../environments/environment.development';

@Injectable({
    providedIn: 'root'
})
export class ProductService {

    private readonly apiUrl = `${environment.baseUrl}/products`;

    constructor(private http: HttpClient) { }

    getProducts(): Observable<Product[]> {
        return this.http.get<ApiResponse<Product[]>>(this.apiUrl).pipe(
            map(res => res.payload as Product[])
        );
    }

    getProductById(id: number): Observable<Product> {
        return this.http.get<ApiResponse<Product>>(`${this.apiUrl}/${id}`).pipe(
            map(res => res.payload as Product)
        );
    }

    getProductsByStoreId(storeId: number): Observable<Product[]> {
        return this.http.get<ApiResponse<Product[]>>(`${this.apiUrl}/store/${storeId}`).pipe(
            map(res => res.payload as Product[])
        );
    }
}


