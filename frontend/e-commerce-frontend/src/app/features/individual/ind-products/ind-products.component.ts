import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { ProductService } from '../../../core/services/product.service';
import { Product } from '../../../shared/models/product';

import { CartService } from '../../../core/services/cart.service';
import { ToastService } from '../../../core/services/toast.service';

@Component({
  selector: 'app-ind-products',
  standalone: true,
  imports: [CommonModule, RouterLink],
  templateUrl: './ind-products.component.html',
  styleUrl: './ind-products.component.css'
})
export class IndProductsComponent implements OnInit {
  private productService = inject(ProductService);
  private cartService = inject(CartService);
  private toastService = inject(ToastService);

  products: Product[] = [];
  isLoading = true;
  errorMessage = '';

  ngOnInit(): void {
    this.fetchProducts();
  }

  fetchProducts(): void {
    this.isLoading = true;
    this.productService.getProducts().subscribe({
      next: (data) => {
        this.products = data;
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error fetching products', err);
        this.errorMessage = 'Failed to load products. Please try again later.';
        this.isLoading = false;
      }
    });
  }

  addToCart(product: Product): void {
    if (!product.id) return;
    this.cartService.addItemToCart({ productId: product.id, quantity: 1 }).subscribe({
        next: () => {
            this.toastService.showSuccess(`🛒 ${product.name} added to cart!`);
        }
    });
  }

  getImageUrl(url: string | null | undefined): string {
    if (!url) return 'assets/placeholder-product.png';
    if (url.startsWith('http://') || url.startsWith('https://')) return url;
    if (url.startsWith('assets/images/')) return url;
    return `assets/images/${url}`;
  }
}
