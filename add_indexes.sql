CREATE INDEX idx_product_check_positions_check_id ON prod.product_check_positions (check_id);
CREATE INDEX idx_supplies_products_supply_id ON prod.supplies_products (supply_id);
CREATE INDEX idx_external_supplies_products_ext_supply_id ON prod.external_supplies_products (ext_supply_id);
