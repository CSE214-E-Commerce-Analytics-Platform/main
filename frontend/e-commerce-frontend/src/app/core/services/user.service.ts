import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, map } from 'rxjs';
import { User, UserRequest } from '../../shared/models/user';
import { ApiResponse } from '../../shared/models/api-response';
import { environment } from '../../../environments/environment.development';

@Injectable({
    providedIn: 'root'
})
export class UserService {

    private readonly apiUrl = `${environment.baseUrl}/users`;

    constructor(private http: HttpClient) { }

    findAllUsers(): Observable<User[]> {
        return this.http.get<ApiResponse<User[]>>(this.apiUrl).pipe(
            map(res => res.payload as User[])
        );
    }

    findAllUsersByRole(role: string): Observable<User[]> {
        return this.http.get<ApiResponse<User[]>>(`${this.apiUrl}/all-by-role`, {
            params: { role }
        }).pipe(
            map(res => res.payload as User[])
        );
    }

    findUserById(id: number): Observable<User> {
        return this.http.get<ApiResponse<User>>(`${this.apiUrl}/${id}`).pipe(
            map(res => res.payload as User)
        );
    }

    updateUserById(id: number, user: UserRequest): Observable<User> {
        return this.http.put<ApiResponse<User>>(`${this.apiUrl}/${id}`, user).pipe(
            map(res => res.payload as User)
        );
    }

    deleteUserById(id: number): Observable<void> {
        return this.http.delete<ApiResponse<void>>(`${this.apiUrl}/${id}`).pipe(
            map(() => void 0)
        );
    }
}
