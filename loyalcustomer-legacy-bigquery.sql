SELECT
  MAX(c.fullVisitorId) fullVisitorId,
  MAX(c.productSKU) productSKU,
  MAX(c.productName) productName,
  SUM(c.productQuantity) quantity,
  -- totalValue = totalQuantityOfProductPurchased * productPrice
  (SUM(c.productQuantity) * MAX(c.productPrice)) totalValue,
  COUNT(*) consecutiveWeeksCount,
  DATE(MAX(c.date)) lastWeek
FROM (
  SELECT
    b.fullVisitorId fullVisitorId,
    b.productSKU productSKU,
    b.productName productName,
    b.productQuantity productQuantity,
    b.productPrice productPrice,
    LAG(b.week, 1) OVER (PARTITION BY b.fullVisitorId, b.productSKU ORDER BY b.week) previousWeek,
    b.week currentWeek,
    b.date date
  FROM (
    SELECT
      a.fullVisitorId fullVisitorId,
      a.productSKU productSKU,
      a.week week,
      a.productQuantity productQuantity,
      a.productPrice productPrice,
      a.productName productName,
      a.date date
    FROM (
      SELECT
        fullVisitorId fullVisitorId,
        hits.product.productSKU productSKU,
        INTEGER(IF(WEEK(TIMESTAMP(date)) < 10, CONCAT(STRING(YEAR(TIMESTAMP(date))),'0',STRING(WEEK(TIMESTAMP(date)))), CONCAT(STRING(YEAR(TIMESTAMP(date))),STRING(WEEK(TIMESTAMP(date)))))) week,
        date,
        hits.product.productQuantity productQuantity,
        hits.product.productPrice productPrice,
        hits.product.v2ProductName productName,
      FROM
        [data-to-insights:ecommerce.web_analytics]
      WHERE
        -- Only for Completed Product Purchases
        hits.eCommerceAction.action_type = '6'
        -- Product SKU can't be null
        AND hits.product.productSKU IS NOT NULL
        -- Some quantity has to be purchased
        AND hits.product.productQuantity IS NOT NULL ) a
    ORDER BY
      a.fullVisitorId,
      a.productSKU,
      a.week )b )c
WHERE
  c.previousWeek IS NOT NULL
  -- Condition for consecutive week
  AND ((c.currentWeek - c.previousWeek) = 1
    -- Case for change in year
    OR (c.currentWeek - c.previousWeek) = 49)
GROUP BY
  c.fullVisitorId,
  c.productSKU
ORDER BY
  c.fullVisitorId,
  c.productSKU