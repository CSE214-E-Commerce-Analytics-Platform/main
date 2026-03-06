import { Component, inject, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { CommonModule } from '@angular/common';
import { ProductService } from '../../../core/services/product.service';
import { Product } from '../../../shared/models/product';

@Component({
    selector: 'app-product-detail',
    imports: [CommonModule],
    templateUrl: './product-detail.component.html',
    styleUrl: './product-detail.component.css'
})
export class ProductDetailComponent implements OnInit {
    private route = inject(ActivatedRoute);
    private router = inject(Router);
    private productService = inject(ProductService);

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

    goBack(): void {
        this.router.navigate(['/products']);
    }
}
