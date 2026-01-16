# Case Study: Healthcare Resource Allocation

## Problem Description
The goal is to optimize the allocation of healthcare resources (doctors, nurses, beds, equipment) across multiple hospital departments to minimize wait times and maximize patient throughput, subject to budget and capacity constraints.

### Variables
Each variable $x_i$ represents the quantity of a specific resource allocated to a specific department.
*   $D$: Number of departments (e.g., ER, ICU, Surgery).
*   $R$: Number of resource types (e.g., Doctors, Beds).
*   Total variables $N = D \times R$.

### Objective Function
Minimize the **Total Weighted Wait Time**:
$$ F(x) = \sum_{d=1}^{D} W_d \times \frac{Demand_d}{Capacity(x_d)} $$
Where $Capacity(x_d)$ is a function of allocated resources.

### Constraints
1.  **Budget**: $\sum x_i \times Cost_i \le TotalBudget$
2.  **Minimum Staffing**: $x_{doctor} \ge MinDoctors$

## Implementation with Samyama Optimization

This example demonstrates using the **Jaya Algorithm** (metaphor-less, parameter-less) to solve this problem efficiently in Rust.

### Why Rust?
*   **Performance**: Fitness evaluation involves complex arithmetic. Rust handles this 10-50x faster than Python loops.
*   **Parallelism**: Evaluating 1000 candidate allocation plans happens in parallel automatically via `rayon`.

## Usage
Run the example:
```bash
cargo run --release --example healthcare_demo
```

