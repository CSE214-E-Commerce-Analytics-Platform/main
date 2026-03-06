export interface User {
    id: number;
    email: string;
    roleType: string;
    isActive: boolean;
    storeId?: number;
}