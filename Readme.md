sh -c  "bash src/download.sh"
ALTER TABLE olist.geolocation ADD geo_id serial NOT NULL;
ALTER TABLE olist.geolocation ADD CONSTRAINT geolocation_unique UNIQUE (geo_id);
ALTER TABLE olist.geolocation ADD CONSTRAINT geolocation_pk PRIMARY KEY (geolocation_zip_code_prefix,geo_id);
