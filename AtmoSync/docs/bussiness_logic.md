# AtmoSync — Mathematical & Business Logic Models

**Author:** Member 3 — Superset & Documentation  
**Technical Model Lead:** Member 2 — Snowflake & dbt

---

## 1. Agricultural Commodity Safe Envelopes

Perishable agricultural commodities possess biological storage thresholds defined by cellular respiration and pathogen sensitivity:

| Commodity | Optimal Temp | Max Tolerable Temp | Optimal RH | Max RH | Baseline Shelf Life |
|---|---|---|---|---|---|
| **Avocado (Hass)** | 4.0°C – 7.0°C | 12.0°C | 85% – 90% | 95% | 168 hours (7 days) |
| **Banana (Cavendish)** | 13.0°C – 15.0°C | 18.0°C | 90% – 95% | 98% | 120 hours (5 days) |
| **Tomato (Vine)** | 10.0°C – 13.0°C | 18.0°C | 85% – 90% | 95% | 144 hours (6 days) |
| **Mango (Alphonso)** | 10.0°C – 13.0°C | 16.0°C | 85% – 90% | 95% | 120 hours (5 days) |
| **Apple (Fuji)** | 0.0°C – 4.0°C | 8.0°C | 90% – 95% | 98% | 720 hours (30 days) |

---

## 2. Deterministic Spoilage Risk Score ($R \in [0, 100]$)

The Spoilage Risk Score is computed deterministically in `int_spoilage_risk.sql` through four orthogonal physical penalty terms:

$$R = \min\left(100.0, \; P_{\text{temp}} + P_{\text{humidity}} + P_{\text{vibration}} + P_{\text{compressor}}\right)$$

### 1. Thermal Excursion Penalty ($P_{\text{temp}} \in [0, 50]$)
$$P_{\text{temp}} = \min(50.0, \; \Delta T \times 8.5)$$
where $\Delta T = \max(0, T - T_{\text{opt\_max}}) + \max(0, T_{\text{opt\_min}} - T)$.

### 2. Moisture Penalty ($P_{\text{humidity}} \in [0, 20]$)
$$P_{\text{humidity}} = \begin{cases} 20.0 & \text{if } H > H_{\text{opt\_max}} \text{ or } H < H_{\text{opt\_min}} \\ 0.0 & \text{otherwise} \end{cases}$$

### 3. Mechanical Bruising Penalty ($P_{\text{vibration}} \in [0, 20]$)
High-frequency road shock shears plant cell walls, releasing ethylene gas and accelerating decay:
$$P_{\text{vibration}} = \min\left(20.0, \; \max\left(0, (V - 0.15) \times 60.0\right)\right)$$

### 4. Active Machinery Status Penalty ($P_{\text{compressor}} \in [0, 25]$)
- `FAILURE`: $25.0$ pts
- `DEGRADED`: $12.0$ pts
- `NORMAL`: $0.0$ pts

### Risk Categorization:
- $0 \le R \le 25$: **Low Risk** (Nominal cruise conditions)
- $26 \le R \le 50$: **Moderate Risk** (Minor thermal deviation)
- $51 \le R \le 75$: **High Risk** (Quality decay active; intervention recommended)
- $76 \le R \le 100$: **Critical Risk** (Immediate cargo loss imminent)

---

## 3. Estimated Time-to-Spoilage ($T_{\text{spoil}}$ in hours)

Consistent with biological kinetics where cellular degradation accelerates non-linearly under thermal stress:

$$T_{\text{spoil}} = \max\left(1.0, \; T_{\text{baseline}} \times \left(1.0 - \frac{R}{100.0}\right)^{1.85}\right)$$

For Avocado ($T_{\text{baseline}} = 168\text{ hrs}$):
- At $R = 10$ (Nominal): $T_{\text{spoil}} \approx 168 \times (0.90)^{1.85} \approx 138\text{ hrs}$
- At $R = 50$ (Moderate): $T_{\text{spoil}} \approx 168 \times (0.50)^{1.85} \approx 46\text{ hrs}$
- At $R = 88.5$ (Critical): $T_{\text{spoil}} \approx 168 \times (0.115)^{1.85} \approx 9.5\text{ hrs}$

---

## 4. Spoilage Arbitrage Financial Formulation

Let:
- $Q$: Total cargo payload (kg)
- $D_{\text{curr}}$: Planned destination market
- $P_{\text{curr}}$: Spot price at current destination (₹/kg)
- $t_{\text{curr}}$: Remaining transit time to current destination (hours)
- $M_j$: Candidate alternative wholesale terminal market
- $P_j$: Spot price at alternative terminal $M_j$ (₹/kg)
- $t_j$: Transit time to alternative terminal $M_j$ (hours)
- $C_j$: Detour freight & cooling cost to $M_j$ (₹)
- $T_{\text{spoil}}$: Estimated time-to-spoilage (hours)

### Projected Spoilage Loss Fraction ($\phi$)
Upon arrival at any terminal $k$:
$$\phi_k = \begin{cases}
\min\left(1.0, \; 0.35 + 0.65 \times \frac{t_k - T_{\text{spoil}}}{t_k}\right) & \text{if } t_k \ge T_{\text{spoil}} \\
\frac{R}{100.0} \times 0.15 & \text{if } t_k < T_{\text{spoil}}
\end{cases}$$

### Net Realizable Value (NRV)
$$\text{NRV}_{\text{curr}} = Q \times P_{\text{curr}} \times (1.0 - \phi_{\text{curr}})$$
$$\text{NRV}_j = \left(Q \times P_j \times (1.0 - \phi_j)\right) - C_j$$

### Spoilage Arbitrage
$$\text{Spoilage Arbitrage}_j = \max(0.0, \; \text{NRV}_j - \text{NRV}_{\text{curr}})$$

### Decision Logic
A container is flagged with `is_reroute_recommended = TRUE` if and only if:
1. $\text{Spoilage Arbitrage}_j > 0$
2. Risk Category is **High Risk** or **Critical Risk**
3. Alternative terminal is reachable within remaining shelf life ($t_j < T_{\text{spoil}}$)