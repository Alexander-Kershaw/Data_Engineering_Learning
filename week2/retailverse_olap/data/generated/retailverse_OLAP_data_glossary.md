# Retailverse Data Glossary

## Purpose

This glossary documents the current Retailverse OLAP dimensional model. It defines each table, its grain, the meaning of each column, key types, special values, and important modelling notes.

---

## Table Summary

| Table | Type | Grain | Primary Purpose | Main Context |
|---|---|---|---|---|
| `dim_date` | Dimension | One row per calendar date | Reusable calendar reporting attributes | Year, quarter, month, week, weekend |
| `dim_customer` | Dimension | One row per warehouse customer record | Customer descriptive and loyalty context | Identity, geography, signup, loyalty |
| `dim_product` | Dimension | One row per product | Product descriptive hierarchy | Category, subcategory, brand, price/cost |
| `dim_store` | Dimension | One row per physical store plus special members | Store geography and retail context | Region, city, store type |
| `dim_channel` | Dimension | One row per standardised sales channel | Canonical sales-channel vocabulary | STORE, ECOMMERCE, MOBILE |
| `dim_supplier` | Dimension | One row per supplier | Supplier and replenishment context | Lead time, reliability, MOQ |
| `fact_sales` | Fact | One row per source order item | Sales, revenue, discount, cost and profit analysis | Date, customer, product, store, channel |
| `fact_returns` | Fact | One row per return event for an original order item | Refund and returns analysis | Date, customer, product |
| `fact_inventory` | Fact | One row per product-store inventory snapshot date | Availability and stock-risk analysis | Date, product, store, supplier |
| `fact_stock_count` | Fact | One row per physical stock-count observation | Inventory variance and exception analysis | Date, product, store |

---

# Dimensions

## `dim_date`

**Grain:** One row per calendar date.

| Column | Data Type | Key Type | Nullable | Meaning | Notes |
|---|---|---|---|---|---|
| `date_key` | INT | PK | No | Warehouse date key in `YYYYMMDD` integer form | Example: `20250131` |
| `full_date` | DATE |  | No | Actual calendar date | Example: `2025-01-31` |
| `year` | INT |  | No | Calendar year | Example: `2025` |
| `quarter` | INT |  | No | Calendar quarter number | Example: `1` |
| `month_number` | INT |  | No | Calendar month number | Example: `1` |
| `month_name` | STRING |  | No | Calendar month name | Example: `January` |
| `week_number` | INT |  | No | Calendar week number |  |
| `day_of_month` | INT |  | No | Day number within the month |  |
| `day_name` | STRING |  | No | Name of weekday | Example: `Friday` |
| `is_weekend` | BOOLEAN |  | No | Whether the date falls on a weekend |  |
| `year_month` | STRING |  | No | Year-month reporting label | Example: `2025-01` |

---

## `dim_customer`

**Grain:** One row per warehouse customer record.

| Column | Data Type | Key Type | Nullable | Meaning | Notes |
|---|---|---|---|---|---|
| `customer_key` | BIGINT | PK | No | Warehouse surrogate key for a customer | `-1 = Unknown` |
| `customer_id` | STRING | Natural Key | No | Customer identifier from source systems | Example: `C000123` |
| `first_name` | STRING |  | Yes | Customer first name |  |
| `last_name` | STRING |  | Yes | Customer surname |  |
| `email` | STRING |  | Yes | Customer email address | May be null or deliberately defective in raw data |
| `phone` | STRING |  | Yes | Customer telephone number |  |
| `city` | STRING |  | Yes | Customer city |  |
| `postcode` | STRING |  | Yes | Customer postcode |  |
| `signup_date` | DATE |  | Yes | Date customer first registered |  |
| `source_system` | STRING |  | Yes | Operational system the customer originated from |  |
| `loyalty_member` | BOOLEAN |  | No | Whether customer is recorded as a loyalty member |  |
| `loyalty_id` | STRING |  | Yes | Loyalty programme identifier | Null for non-members |
| `loyalty_join_date` | DATE |  | Yes | Date customer joined the loyalty programme |  |

**Important:** Synthetic `behaviour_segment` is intentionally not used in this dimension for retention analysis. Retention categories should be derived from actual transaction history.

---

## `dim_product`

**Grain:** One row per product.

| Column | Data Type | Key Type | Nullable | Meaning | Notes |
|---|---|---|---|---|---|
| `product_key` | BIGINT | PK | No | Warehouse surrogate key for product | `-1 = Unknown` |
| `product_id` | STRING | Natural Key | No | Source product identifier | Example: `P000241` |
| `sku` | STRING |  | No | Stock keeping unit |  |
| `product_name` | STRING |  | No | Descriptive product name |  |
| `category` | STRING |  | No | Top-level product category | Example: `Electronics` |
| `subcategory` | STRING |  | No | Lower-level product grouping |  |
| `brand` | STRING |  | Yes | Product brand |  |
| `unit_price` | DECIMAL(12,2) |  | No | Current listed unit selling price | Historical sale price is stored in `fact_sales` |
| `unit_cost` | DECIMAL(12,2) |  | No | Current product unit cost | Used as a simplifying cost assumption in `fact_sales` |
| `active_flag` | BOOLEAN |  | No | Whether product is active in the source catalogue |  |

---

## `dim_store`

**Grain:** One row per physical store plus special warehouse members.

| Column | Data Type | Key Type | Nullable | Meaning | Notes |
|---|---|---|---|---|---|
| `store_key` | BIGINT | PK | No | Warehouse surrogate key for store | `-1 = Unknown`, `0 = Not Applicable`, positive values = real stores |
| `store_id` | STRING | Natural Key | No | Source store identifier | Example: `S013` |
| `store_name` | STRING |  | No | Store display name |  |
| `region` | STRING |  | Yes | Geographic reporting region |  |
| `city` | STRING |  | Yes | Store city |  |
| `postcode` | STRING |  | Yes | Store postcode |  |
| `store_type` | STRING |  | Yes | Store format or type |  |
| `opened_date` | DATE |  | Yes | Date store opened |  |

---

## `dim_channel`

**Grain:** One row per standardised sales channel.

| Column | Data Type | Key Type | Nullable | Meaning | Notes |
|---|---|---|---|---|---|
| `channel_key` | BIGINT | PK | No | Warehouse surrogate key for sales channel | `-1 = Unknown` |
| `channel_code` | STRING | Natural Key | No | Canonical sales-channel code | `STORE`, `ECOMMERCE`, `MOBILE` |
| `channel_name` | STRING |  | No | Human-readable channel name | Example: `Physical Store` |

Dirty source values such as `POS`, `Retail`, `WEB`, `ONLINE`, and `APP` are mapped into canonical channel codes.

---

## `dim_supplier`

**Grain:** One row per supplier.

| Column | Data Type | Key Type | Nullable | Meaning | Notes |
|---|---|---|---|---|---|
| `supplier_key` | BIGINT | PK | No | Warehouse surrogate key for supplier | `-1 = Unknown` |
| `supplier_id` | STRING | Natural Key | No | Source supplier identifier |  |
| `supplier_name` | STRING |  | No | Supplier name |  |
| `country` | STRING |  | Yes | Supplier country |  |
| `lead_time_days` | INT |  | Yes | Expected supplier replenishment lead time in days | Deliberate negative lead-time defects exist in raw data |
| `reliability_score` | DECIMAL |  | Yes | Supplier reliability measure | Synthetic values are approximately `0.80–0.995` |
| `minimum_order_qty` | INT |  | Yes | Minimum supplier order quantity |  |
| `active_flag` | BOOLEAN |  | No | Whether supplier is active |  |

---

# Facts

## `fct_sales`

**Grain:** One row per source order item.

| Column | Data Type | Key Type | Nullable | Meaning | Notes |
|---|---|---|---|---|---|
| `order_item_id` | STRING | Degenerate Key | No | Identifier of source order line | Deliberate duplicate IDs may exist |
| `order_id` | STRING | Degenerate Key | No | Identifier of parent order |  |
| `line_number` | INT |  | No | Line position within the order |  |
| `date_key` | INT | FK | No | Date of the sale | Joins `dim_date.date_key` |
| `customer_key` | BIGINT | FK | No | Customer surrogate key | `-1 = Unknown customer` |
| `product_key` | BIGINT | FK | No | Product surrogate key | `-1 = Unknown product` |
| `store_key` | BIGINT | FK | No | Store surrogate key | `-1 = Unknown`, `0 = Not Applicable` |
| `channel_key` | BIGINT | FK | No | Canonical sales-channel surrogate key | Joins `dim_channel.channel_key` |
| `order_timestamp` | TIMESTAMP |  | No | Timestamp when order occurred |  |
| `order_status` | STRING |  | No | Order lifecycle status | Usually `COMPLETED` or `CANCELLED` |
| `quantity` | INT |  | No | Units purchased on the order line | Deliberate negative and zero-quantity defects exist |
| `unit_price` | DECIMAL(12,2) |  | No | Historical selling price per unit at transaction time |  |
| `discount_pct` | DECIMAL(8,4) |  | No | Discount percentage applied to line | Example: `0.10 = 10%` |
| `gross_amount` | DECIMAL(14,2) |  | No | Pre-discount line value | Normally `quantity × unit_price` |
| `discount_amount` | DECIMAL(14,2) |  | No | Monetary discount applied |  |
| `net_amount` | DECIMAL(14,2) |  | No | Sales value after discount | Before refunds |
| `unit_cost` | DECIMAL(12,2) |  | Yes | Unit cost used for profitability | Currently sourced from product dimension |
| `cost_amount` | DECIMAL(14,2) |  | Yes | Estimated total line cost | `quantity × unit_cost` |
| `profit_amount` | DECIMAL(14,2) |  | Yes | Estimated line profit | `net_amount - cost_amount` |

---

## `fct_returns`

**Grain:** One row per return event for an original order item.

| Column | Data Type | Key Type | Nullable | Meaning | Notes |
|---|---|---|---|---|---|
| `return_id` | STRING | Degenerate Key | No | Identifier of return event |  |
| `original_order_id` | STRING | Degenerate Key | No | Original sale order identifier |  |
| `original_order_item_id` | STRING | Degenerate Key | No | Original sale line identifier |  |
| `return_date_key` | INT | FK | No | Calendar date of return | Joins `dim_date.date_key` |
| `customer_key` | BIGINT | FK | No | Customer associated with returned sale | `-1 = Unknown` |
| `product_key` | BIGINT | FK | No | Returned product | `-1 = Unknown` |
| `returned_quantity` | INT |  | No | Number of units returned | Stored as a positive return quantity |
| `refund_amount` | DECIMAL(14,2) |  | No | Amount refunded to customer |  |
| `return_type` | STRING |  | No | Type of return | `FULL_ORDER`, `FULL_LINE`, `PARTIAL_LINE` |
| `return_reason` | STRING |  | Yes | Synthetic reason for the return |  |

---

## `fct_inventory`

**Grain:** One row per product-store inventory snapshot date.

| Column | Data Type | Key Type | Nullable | Meaning | Notes |
|---|---|---|---|---|---|
| `inventory_snapshot_id` | STRING | Degenerate Key | No | Identifier of inventory snapshot record |  |
| `date_key` | INT | FK | No | Snapshot date | Joins `dim_date` |
| `store_key` | BIGINT | FK | No | Store holding the inventory | Joins `dim_store` |
| `product_key` | BIGINT | FK | No | Product held at store | Joins `dim_product` |
| `supplier_key` | BIGINT | FK | No | Primary supplier for product | Joins `dim_supplier` |
| `on_hand_qty` | INT |  | No | Units recorded as on hand | Negative stock is a deliberate defect |
| `reserved_qty` | INT |  | No | Units reserved for orders or allocation | May exceed on-hand in deliberate defects |
| `available_qty` | INT |  | No | Units available after reservations | `on_hand_qty - reserved_qty` |
| `units_sold_30d` | INT |  | No | Recent 30-day unit sales used for demand context |  |
| `avg_daily_sales` | DECIMAL |  | No | Average daily sales rate |  |
| `lead_time_days` | INT |  | Yes | Expected replenishment lead time |  |
| `days_of_supply` | DECIMAL |  | Yes | Estimated days until available stock is exhausted | Null where demand is zero |

---

## `fct_stock_count`

**Grain:** One row per physical stock-count observation.

| Column | Data Type | Key Type | Nullable | Meaning | Notes |
|---|---|---|---|---|---|
| `stock_count_id` | STRING | Degenerate Key | No | Identifier of physical count event |  |
| `inventory_snapshot_id` | STRING | Reference | No | Related inventory snapshot identifier |  |
| `date_key` | INT | FK | No | Date of physical count | Joins `dim_date` |
| `store_key` | BIGINT | FK | No | Store where count occurred | Joins `dim_store` |
| `product_key` | BIGINT | FK | No | Product counted | Joins `dim_product` |
| `system_qty` | INT |  | No | Quantity recorded by inventory system |  |
| `physical_qty` | INT |  | No | Quantity physically counted |  |
| `variance_units` | INT |  | No | Difference between physical and system quantity | `physical_qty - system_qty` |
| `variance_pct` | DECIMAL |  | Yes | Variance expressed relative to system quantity | Null when `system_qty = 0` |

---

# Special Values and Modelling Rules

| Table / Column | Value | Meaning | Interpretation |
|---|---|---|---|
| All surrogate dimension keys | `-1` | Unknown / failed lookup | The business event is retained, but the related dimension record could not be resolved |
| `dim_store.store_key` / `fact_sales.store_key` | `0` | Not Applicable | Used where a physical store genuinely does not apply, especially ecommerce and mobile sales |
| `dim_store.store_key` / `fact_sales.store_key` | `> 0` | Known physical store | Normal warehouse surrogate key |
| `fact_inventory.days_of_supply` | `NULL` | No meaningful days-of-supply estimate | Expected when average demand is zero; not equivalent to infinite supply |
| `fact_stock_count.variance_pct` | `NULL` when `system_qty = 0` | Percentage variance undefined | Prevents division by zero while preserving `variance_units` |
| `fact_sales.order_status` | `COMPLETED` | Completed business sale | Normally included in revenue analysis |
| `fact_sales.order_status` | `CANCELLED` | Cancelled order | Normally excluded from completed-sales KPIs |

---

# Key Modelling Notes

- Facts use surrogate keys to join to dimensions.
- Natural source identifiers are retained in dimensions and, where useful, as degenerate identifiers in facts.
- `-1` means unknown or failed lookup.
- `0` in the store dimension means not applicable, not unknown.
- The synthetic dataset deliberately contains data quality defects for later testing.
- Valid unusual records should not automatically be treated as defects.


