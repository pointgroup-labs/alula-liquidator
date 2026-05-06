# AMM Max Swap Estimator (Constant Product Model)

This document outlines the algorithm for calculating the maximum input amount ($\Delta x$) possible for a swap without exceeding a specific **Price Impact** limit ($S$), using only the standard `get_amount_out` method. It is designed for $x \cdot y = k$ (Constant Product) AMMs.

---

## 1. Parameters & Inputs
* **$S$**: Maximum allowed price impact as a decimal (e.g., `0.01` for 1%).
* **$f$**: Pool fee as a decimal (e.g., `0.003` for 0.3%).
* **$\gamma$**: Fee multiplier, where $\gamma = 1 - f$ (e.g., `0.997`).

---

## 2. Step-by-Step Algorithm

### Step 1: Probe the Pool (State Discovery)
Since pool reserves ($x, y$) are not directly accessed, we derive the "virtual reserve" by observing the curve's curvature over two tiny increments.

1.  Call `get_amount_out(1)` and store as $y_1$.
2.  Call `get_amount_out(2)` and store as $y_2$.

*Note: Use the smallest unit (e.g., 1 atom) that returns a non-zero value.*

### Step 2: Derive Virtual Reserve ($x$)
Using the relationship between the two probes on the hyperbola, calculate the estimated reserve of the token you are selling (token $x$):

$$x = \frac{2\gamma \cdot (y_2 - y_1)}{2y_1 - y_2}$$

### Step 3: Solve for Maximum Input ($\Delta x$)
Calculate the maximum amount you can swap in to hit your target price impact exactly. In this model, price impact is defined as the deviation of the effective price from the spot price.

$$\Delta x = \frac{S \cdot x}{\gamma \cdot (1 - S)}$$

### Step 4: Verification & Execution
1.  **Expected Output:** Call `get_amount_out(\Delta x)` to get the current predicted return ($Expected\_Y$).
2.  **Slippage Guard:** Set your `min_amount_out` for the swap by applying a small safety buffer (e.g., 0.1% to 0.5%) to $Expected\_Y$ to account for minor fluctuations in the pool between calculation and execution.

---

## 3. Implementation Summary

| Component | Formula |
| :--- | :--- |
| **Fee Multiplier** | $\gamma = 1 - \text{fee}$ |
| **Reserve Estimator** | $x = \frac{2\gamma(y_2 - y_1)}{2y_1 - y_2}$ |
| **Max Input Solver** | $\Delta x = \frac{S \cdot x}{\gamma(1 - S)}$ |

---

## 4. Technical Considerations (Fixed-Point Math)
When implementing in environments like Rust or Solidity:

* **Precision:** Always multiply numerators before dividing to avoid precision loss.
* **Scaling:** If $y_1$ and $y_2$ are very small, the denominator $(2y_1 - y_2)$ may be prone to rounding errors. It is often safer to use slightly larger probes (e.g., $10^6$ and $2 \cdot 10^6$) and scale the result accordingly.
* **Rounding:** Use "Round Up" for the reserve calculation to ensure a conservative estimate of liquidity depth.
