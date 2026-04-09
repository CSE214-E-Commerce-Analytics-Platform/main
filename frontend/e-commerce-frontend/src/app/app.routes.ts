import { Routes } from '@angular/router';
import { LoginComponent } from './features/auth/login/login.component';
import { ProductListComponent } from './features/products/product-list/product-list.component';
import { ProductDetailComponent } from './features/products/product-detail/product-detail.component';
import { authGuard } from './core/guards/auth.guard';
import { roleGuard } from './core/guards/role.guard';
import { ForbiddenComponent } from './features/auth/forbidden/forbidden.component';
import { RegisterComponent } from './features/auth/register/register.component';
import { ForgotPasswordComponent } from './features/auth/forgot-password/forgot-password.component';
import { ResetPasswordComponent } from './features/auth/reset-password/reset-password.component';
import { VerifyEmailComponent } from './features/auth/verify-email/verify-email.component';

import { AdminLayoutComponent } from './features/admin/admin-layout/admin-layout.component';
import { AdminDashboardComponent } from './features/admin/admin-dashboard/admin-dashboard.component';
import { AdminUsersComponent } from './features/admin/admin-users/admin-users.component';
import { AdminStoresComponent } from './features/admin/admin-stores/admin-stores.component';
import { AdminCategoriesComponent } from './features/admin/admin-categories/admin-categories.component';
import { AdminSettingsComponent } from './features/admin/admin-settings/admin-settings.component';

import { CorporateLayoutComponent } from './features/corporate/corporate-layout/corporate-layout.component';
import { CorpDashboardComponent } from './features/corporate/corp-dashboard/corp-dashboard.component';
import { CorpProductsComponent } from './features/corporate/corp-products/corp-products.component';
import { CorpInventoryComponent } from './features/corporate/corp-inventory/corp-inventory.component';
import { CorpOrdersComponent } from './features/corporate/corp-orders/corp-orders.component';
import { CorpAnalyticsComponent } from './features/corporate/corp-analytics/corp-analytics.component';
import { CorpReviewsComponent } from './features/corporate/corp-reviews/corp-reviews.component';

import { IndividualLayoutComponent } from './features/individual/individual-layout/individual-layout.component';
import { IndProductsComponent } from './features/individual/ind-products/ind-products.component';
import { IndCartComponent } from './features/individual/ind-cart/ind-cart.component';
import { IndOrdersComponent } from './features/individual/ind-orders/ind-orders.component';
import { IndHistoryComponent } from './features/individual/ind-history/ind-history.component';
import { IndReviewsComponent } from './features/individual/ind-reviews/ind-reviews.component';
import { IndAnalyticsComponent } from './features/individual/ind-analytics/ind-analytics.component';
import { IndProfileComponent } from './features/individual/ind-profile/ind-profile.component';

import { IndStoresComponent } from './features/individual/ind-stores/ind-stores.component';
import { IndStoreDetailComponent } from './features/individual/ind-store-detail/ind-store-detail.component';

export const routes: Routes = [
    { path: '', redirectTo: 'login', pathMatch: 'full' },
    { path: 'login', component: LoginComponent },
    { path: 'register', component: RegisterComponent },
    { path: 'forgot-password', component: ForgotPasswordComponent },
    { path: 'reset-password', component: ResetPasswordComponent },
    { path: 'verify-email', component: VerifyEmailComponent },

    // Individual (Müşteri) Rotaları
    {
        path: 'individual',
        component: IndividualLayoutComponent,
        canActivate: [authGuard, roleGuard],
        data: { roles: ['INDIVIDUAL'] },
        children: [
            { path: 'products', component: IndProductsComponent },
            { path: 'products/:id', component: ProductDetailComponent },
            { path: 'stores', component: IndStoresComponent },
            { path: 'stores/:id', component: IndStoreDetailComponent },
            { path: 'cart', component: IndCartComponent },
            { path: 'orders', component: IndOrdersComponent },
            { path: 'history', component: IndHistoryComponent },
            { path: 'reviews', component: IndReviewsComponent },
            { path: 'analytics', component: IndAnalyticsComponent },
            { path: 'profile', component: IndProfileComponent },
            { path: '', redirectTo: 'products', pathMatch: 'full' }
        ]
    },

    // Admin Rotaları
    {
        path: 'admin',
        component: AdminLayoutComponent,
        canActivate: [authGuard, roleGuard],
        data: { roles: ['ADMIN'] },
        children: [
            { path: 'dashboard', component: AdminDashboardComponent },
            { path: 'users', component: AdminUsersComponent },
            { path: 'stores', component: AdminStoresComponent },
            { path: 'stores/:id', component: IndStoreDetailComponent },
            { path: 'products/:id', component: ProductDetailComponent },
            { path: 'categories', component: AdminCategoriesComponent },
            { path: 'settings', component: AdminSettingsComponent },
            { path: '', redirectTo: 'dashboard', pathMatch: 'full' }
        ]
    },

    // Corporate (Mağaza) Rotaları
    {
        path: 'corporate',
        component: CorporateLayoutComponent,
        canActivate: [authGuard, roleGuard],
        data: { roles: ['CORPORATE'] },
        children: [
            { path: 'dashboard', component: CorpDashboardComponent },
            { path: 'products', component: CorpProductsComponent },
            { path: 'inventory', component: CorpInventoryComponent },
            { path: 'orders', component: CorpOrdersComponent },
            { path: 'analytics', component: CorpAnalyticsComponent },
            { path: 'reviews', component: CorpReviewsComponent },
            { path: '', redirectTo: 'dashboard', pathMatch: 'full' }
        ]
    },

    // Erişim engeli sayfası
    { path: 'forbidden', component: ForbiddenComponent },
    { path: '**', redirectTo: 'login' }
];

