import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';
import { Category } from '../../shared/models/category';
import { ApiResponse } from '../../shared/models/api-response';
import { environment } from '../../../environments/environment.development';

@Injectable({
    providedIn: 'root'
})
export class CategoryService {

    private readonly apiUrl = `${environment.baseUrl}/categories`;

    constructor(private http: HttpClient) { }

    getAllCategories(): Observable<Category[]> {
        return this.http.get<ApiResponse<Category[]>>(this.apiUrl).pipe(
            map(res => res.payload as Category[])
        );
    }
}
