-- The pipeline BEFORE your change (current production logic).
-- Revenue per paid order.
SELECT
    order_id,
    amount,
    status
FROM orders
WHERE status = 'paid'
