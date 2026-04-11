import { Component, inject, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Location, CommonModule } from '@angular/common';
import { ProductService } from '../../../core/services/product.service';
import { CartService } from '../../../core/services/cart.service';
import { ToastService } from '../../../core/services/toast.service';
import { Product } from '../../../shared/models/product';

@Component({
    selector: 'app-product-detail',
    imports: [CommonModule],
    templateUrl: './product-detail.component.html',
    styleUrl: './product-detail.component.css'
})
export class ProductDetailComponent implements OnInit {
    private route = inject(ActivatedRoute);
    private location = inject(Location);
    private productService = inject(ProductService);
    private cartService = inject(CartService);
    private toastService = inject(ToastService);

    product: Product | null = null;
    isLoading = true;
    errorMessage = '';

    ngOnInit(): void {
        const id = Number(this.route.snapshot.paramMap.get('id'));
        if (id) {
            this.productService.getProductById(id).subscribe({
                next: (data) => {
                    this.product = data;
                    this.isLoading = false;
                },
                error: (err) => {
                    this.errorMessage = 'Product not found or failed to load.';
                    this.isLoading = false;
                }
            });
        }
    }

    addToCart(productId: number | undefined) {
        if (!productId) return;
        this.cartService.addItemToCart({ productId, quantity: 1 }).subscribe({
            next: () => {
                this.toastService.showSuccess(`🛒 Product successfully added to cart!`);
            }
        });
    }

    goBack(): void {
        this.location.back();
    }
}
