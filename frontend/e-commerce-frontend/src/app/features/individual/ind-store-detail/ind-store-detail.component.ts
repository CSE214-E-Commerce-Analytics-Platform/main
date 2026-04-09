import { Component, OnInit, inject } from '@angular/core';
import { CommonModule, Location } from '@angular/common';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { ProductService } from '../../../core/services/product.service';
import { StoreService } from '../../../core/services/store.service';
import { Product } from '../../../shared/models/product';
import { Store } from '../../../shared/models/store';
import { AuthService } from '../../../core/services/auth.service';

@Component({
  selector: 'app-ind-store-detail',
  standalone: true,
  imports: [CommonModule, RouterLink],
  templateUrl: './ind-store-detail.component.html',
  styleUrl: './ind-store-detail.component.css'
})
export class IndStoreDetailComponent implements OnInit {
  private route = inject(ActivatedRoute);
  private location = inject(Location);
  private productService = inject(ProductService);
  private storeService = inject(StoreService);
  private authService = inject(AuthService);

  storeId: number | null = null;
  store: Store | null = null;
  products: Product[] = [];
  userRole: string | null = null;
  
  isLoading = true;
  errorMessage = '';

  ngOnInit(): void {
    this.userRole = this.authService.getRole();
    const idParam = this.route.snapshot.paramMap.get('id');
    if (idParam) {
      this.storeId = Number(idParam);
      this.fetchStoreData();
    } else {
      this.errorMessage = 'Invalid store ID.';
      this.isLoading = false;
    }
  }

  fetchStoreData(): void {
    if (!this.storeId) return;
    
    this.isLoading = true;
    
    // Fetch store details
    this.storeService.getStoreById(this.storeId).subscribe({
      next: (storeData) => {
        this.store = storeData;
        // Then fetch products
        this.fetchProducts();
      },
      error: (err) => {
        console.error('Error fetching store', err);
        this.errorMessage = 'Failed to load store information.';
        this.isLoading = false;
      }
    });
  }

  fetchProducts(): void {
    if (!this.storeId) return;

    this.productService.getProductsByStoreId(this.storeId).subscribe({
        next: (data) => {
            this.products = data;
            this.isLoading = false;
        },
        error: (err) => {
            console.error('Error fetching store products', err);
            this.errorMessage = 'Failed to load store products.';
            this.isLoading = false;
        }
    });
  }

  addToCart(product: Product): void {
    alert(`${product.name} added to cart!`);
  }

  getImageUrl(url: string | null | undefined): string {
    if (!url) return 'assets/placeholder-product.png';
    if (url.startsWith('http://') || url.startsWith('https://')) return url;
    if (url.startsWith('assets/images/')) return url;
    return `assets/images/${url}`;
  }

  goBack(): void {
    this.location.back();
  }
}
