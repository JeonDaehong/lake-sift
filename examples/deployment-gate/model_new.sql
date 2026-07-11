-- The pipeline AFTER your change (the PR under review):
--   * include refunded orders too (was: paid only)  -> shows up as ADDED rows
--   * round amount to whole units                    -> shows up as CHANGED cells
SELECT
    order_id,
    round(amount) AS amount,
    status
FROM orders
WHERE status IN ('paid', 'refunded')
