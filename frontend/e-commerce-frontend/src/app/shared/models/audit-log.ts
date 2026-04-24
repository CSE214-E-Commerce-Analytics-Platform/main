import { BaseDto } from './base-dto';

export interface DtoAuditLog extends BaseDto {
    userId: number;
    userRole: string;
    action: string;
    details: string;
}

export interface RestPageableRequest {
    pageNumber: number;
    pageSize: number;
    columnName?: string;
    asc?: boolean;
}

export interface RestPageableEntity<T> {
    content: T[];
    pageNumber: number;
    pageSize: number;
    totalElement: number;
}
