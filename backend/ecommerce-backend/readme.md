# 🛒 E-Commerce RESTful API Backend

A robust, secure, and scalable e-commerce backend API built with Spring Boot. This system is designed with a multi-vendor/marketplace architecture in mind, featuring advanced role-based access control (RBAC), stateless JWT authentication, and strict resource ownership validation.

## ✨ Key Features

* **Advanced Security & Authorization:** * Custom resource ownership checks using Spring Expression Language (SpEL) and `@PreAuthorize`.
    * Users can only update or delete products and stores that they explicitly own.
* **Stateless Authentication Lifecycle:**
    * Implemented a secure dual-token system: Short-lived **Access Tokens (15 mins)** and long-lived **Refresh Tokens (7 days)**.
    * Designed to support "Silent Refresh" mechanisms on the frontend.
* **Role-Based Access Control (RBAC):**
    * Segregated user roles. Only users with the `CORPORATE` authority can create and manage stores.
* **Layered Architecture & Data Transfer Objects (DTO):**
    * Strict separation of concerns (Controller, Service, Repository layers).
    * Entities are never exposed directly to the client; all incoming and outgoing data is mapped through validated DTOs.
* **Robust Data Validation & Integrity:**
    * Built-in request validation (`@Valid`, `@NotBlank`, `@Positive`).
    * Database-level constraints and business logic checks (e.g., SKU uniqueness validation across the platform).

## 🛠️ Tech Stack

* **Core Framework:** Java 17+ / Spring Boot 3.x
* **Security:** Spring Security, JSON Web Tokens (JWT)
* **Data Access & ORM:** Spring Data JPA, Hibernate
* **Database:** Relational Database (MySQL / PostgreSQL / H2)
* **Utilities:** Lombok (Boilerplate reduction), Spring Boot Validation (Hibernate Validator)
* **Build Tool:** Maven

## 📦 Core Modules & Entities

* **User & Auth:** Manages user registration, login, and token generation/refreshing.
* **Store (Vendor):** Represents a seller's shop. A `CORPORATE` user can manage their own stores.
* **Product:** Represents items for sale, strictly tied to a specific `Store` and `Category`.
* **Category:** Hierarchical classification for products.

## 🔗 API Endpoints Overview

Here is a high-level overview of the exposed REST endpoints:

### Authentication (`/api/auth`)
| Method | Endpoint | Description | Access |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/auth/login` | Authenticates user and returns JWT pair | Public |
| `POST` | `/api/auth/register` | Registers a new user | Public |
| `POST` | `/api/auth/refresh` | Generates a new Access Token via Refresh Token | Public |

### Stores (`/api/stores`)
| Method | Endpoint | Description | Access |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/stores` | Retrieves all active stores | Public |
| `GET` | `/api/stores/{id}` | Retrieves a specific store by ID | Public |
| `GET` | `/api/stores/my-stores` | Retrieves stores owned by the authenticated user | `CORPORATE` |
| `POST` | `/api/stores` | Creates a new store | `CORPORATE` |
| `PUT` | `/api/stores/{id}` | Updates a store (Ownership required) | `CORPORATE` |
| `DELETE` | `/api/stores/{id}` | Deletes a store (Ownership required) | `CORPORATE` |

### Products (`/api/products`)
| Method | Endpoint | Description | Access |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/products` | Retrieves all products | Public |
| `GET` | `/api/products/{id}` | Retrieves a specific product by ID | Public |
| `GET` | `/api/products/store/{id}` | Retrieves all products for a specific store | Public |
| `POST` | `/api/products` | Adds a product to an owned store | `CORPORATE` |
| `PUT` | `/api/products/{id}` | Updates a product (Ownership required) | `CORPORATE` |
| `DELETE` | `/api/products/{id}` | Deletes a product (Ownership required) | `CORPORATE` |

## 🛡️ Security Architecture Highlight

This API implements a zero-trust model at the service layer. Example of the declarative security approach used for resource ownership:

```java
@PutMapping("/{id}")
@PreAuthorize("@securityService.isProductOwner(#currentUser.id, #id)")
public RootEntity<DtoProduct> updateProductById(@PathVariable Long id, 
                                                @Valid @RequestBody DtoProductRequest input,
                                                @AuthenticationPrincipal User currentUser) {
    return ok(productService.updateProductById(id, input, currentUser.getId()));
}
```

*Before the request even reaches the service layer, the custom `securityService` verifies against the database that the authenticated user is the true owner of the store associated with the product.*

## 🚀 Getting Started

### Prerequisites
* Java 17 or higher
* Maven 3.6+
* Your preferred relational database (Ensure connection details are updated in `application.properties` or `.env`)

### Installation

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/yourusername/ecommerce-backend.git](https://github.com/yourusername/ecommerce-backend.git)
   cd ecommerce-backend

2. **Configure Environment Variables:**
   Create an `application-secret.yml` or set environment variables for your database credentials and JWT Secret Key.

3. **Build the project:**
   ```bash
   mvn clean install

3. **Run the application:**
    ```bash
    mvn spring-boot:run
The API will be available at http://localhost:8080.