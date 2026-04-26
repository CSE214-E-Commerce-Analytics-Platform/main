import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';
import { environment } from '../../../environments/environment.development';

export interface ChatHistoryItem {
    id: string;
    title: string;
    initialQuery: string;
    userId: number;
    createdAt: string;
    updatedAt: string;
}

interface ApiResponse<T> {
    status: number;
    payload: T;
    errorMessage?: string;
}

@Injectable({
    providedIn: 'root'
})
export class ChatHistoryService {

    private readonly baseUrl = `${environment.baseUrl}/v1/history`;

    constructor(private http: HttpClient) {}

    getAll(): Observable<ChatHistoryItem[]> {
        return this.http.get<ApiResponse<ChatHistoryItem[]>>(this.baseUrl).pipe(
            map(res => res.payload ?? [])
        );
    }

    create(title: string, initialQuery: string): Observable<ChatHistoryItem> {
        return this.http.post<ApiResponse<ChatHistoryItem>>(this.baseUrl, { title, initialQuery }).pipe(
            map(res => res.payload)
        );
    }

    updateTitle(id: string, title: string): Observable<ChatHistoryItem> {
        return this.http.patch<ApiResponse<ChatHistoryItem>>(`${this.baseUrl}/${id}/title`, { title }).pipe(
            map(res => res.payload)
        );
    }

    delete(id: string): Observable<void> {
        return this.http.delete<void>(`${this.baseUrl}/${id}`);
    }
}
