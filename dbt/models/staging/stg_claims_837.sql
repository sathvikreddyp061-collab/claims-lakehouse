-- Stage the bronze EDI 837 envelope into a typed claim header.
-- The raw `edi_837` text is kept on the row for lineage / RCA.

with source as (
    select
        claim_id,
        subscriber_id,
        receiver_id,
        cast(service_date as date)         as service_date,
        cast(total_charge as decimal(12,2)) as total_charge,
        edi_837,
        cast(ingest_ts as timestamp)        as ingest_ts,
        kafka_partition,
        kafka_offset
    from {{ source('bronze', 'claims_edi837') }}
),

deduped as (
    -- One claim per claim_id, keeping the latest ingest in case of replay
    select *
    from (
        select
            *,
            row_number() over (partition by claim_id order by ingest_ts desc) as rn
        from source
    ) t
    where rn = 1
)

select
    claim_id,
    subscriber_id,
    receiver_id,
    service_date,
    total_charge,
    edi_837,
    ingest_ts,
    kafka_partition,
    kafka_offset,
    -- Cheap structured extraction from the EDI text: pull the rendering NPI
    regexp_extract(edi_837, 'NM1\*82\*1\*[^*]*\*[^*]*\*[^*]*\*[^*]*\*[^*]*\*XX\*([^~*]+)', 1)
        as rendering_provider_npi,
    regexp_extract(edi_837, 'HI\*ABK:([^~*]+)', 1)
        as primary_diagnosis_code,
    regexp_extract(edi_837, 'CLM\*[^*]+\*[^*]+\*\*\*([0-9]+):', 1)
        as place_of_service
from deduped
