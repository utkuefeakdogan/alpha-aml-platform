-- Rebuild addresses: 1–5 per customer (random count), types home/work/mailing/billing/branch.
-- Does not touch customers or transactions.

TRUNCATE aml.customer_addresses;

INSERT INTO aml.customer_addresses (customer_id, city, district, country_code, address_type)
SELECT
    c.customer_id,
    (ARRAY['Istanbul','Ankara','Izmir','Berlin','Munich','Hamburg','Paris','Amsterdam','Vienna','Brussels'])[
        1 + (ABS(hashtext(c.customer_id || ':city')) % 10)
    ],
    'District ' || (1 + ABS(hashtext(c.customer_id || ':d' || s.n::text)) % 20),
    COALESCE(c.country, 'DE'),
    (ARRAY['home','work','mailing','billing','branch'])[
        1 + ((ABS(hashtext(c.customer_id || ':t')) + s.n) % 5)
    ]
FROM aml.customers c
CROSS JOIN LATERAL generate_series(
    1,
    1 + (ABS(hashtext(c.customer_id || ':n')) % 5)
) AS s(n);

ANALYZE aml.customer_addresses;
