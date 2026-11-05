-- Run only on the group's source PostgreSQL, as its owner, by the Docker operator.
-- No password is stored here. Set it interactively with psql's
--   \password marketplace_ingest
-- or the team's approved secret-management workflow.
-- This file creates NO replication slot: create one only when starting CDC.
\set ON_ERROR_STOP on

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'marketplace_ingest') THEN
        CREATE ROLE marketplace_ingest LOGIN REPLICATION;
    END IF;
END
$$;

GRANT CONNECT ON DATABASE marketplace TO marketplace_ingest;
GRANT USAGE ON SCHEMA public TO marketplace_ingest;
GRANT SELECT ON TABLE public.orders, public.order_items, public.review,
    public.product_reviews, public.product, public.customer_shipping,
    public.shipping_details TO marketplace_ingest;

-- Customer/password and saved-card tables are deliberately not granted.
-- Existing roles can have inherited permissions: inspect them before reuse.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'marketplace_bronze_publication') THEN
        CREATE PUBLICATION marketplace_bronze_publication FOR TABLE
            public.orders, public.order_items, public.review, public.product_reviews,
            public.product, public.customer_shipping, public.shipping_details
            WITH (publish = 'insert, update, delete');
    ELSE
        RAISE NOTICE 'Publication exists: inspect pg_publication_tables before using it.';
    END IF;
END
$$;

SELECT pubname, schemaname, tablename FROM pg_publication_tables
WHERE pubname = 'marketplace_bronze_publication' ORDER BY tablename;
