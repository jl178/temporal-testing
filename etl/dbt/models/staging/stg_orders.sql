-- Generated from the canonical model: see schema.yml + build_staging.
{{ config(tags=['orders']) }}
{{ build_staging('raw', 'orders') }}
