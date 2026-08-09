-- Generated from the canonical model: see schema.yml + build_staging.
{{ config(tags=['customers']) }}
{{ build_staging('raw', 'customers') }}
