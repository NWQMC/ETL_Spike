with addresses as (select org_address."ORG_UID" org_uid,
                          address_type."ADDTYP_NAME" address_type,
                          org_address."ORGADD_ADDRESS" address_text,
                          org_address."ORGADD_ADDRESS_SUPPLEMENTAL" supplemental_address_text,
                          org_address."ORGADD_LOCALITY_NAME" locality_name,
                          org_address."ORGADD_POSTAL_CD" postal_code,
                          country.cntry_cd country_code,
                          state.st_cd state_code,
                          county.cnty_fips_cd county_code,
                          row_number() over (partition by org_address."ORG_UID" order by org_address."ORGADD_UID") addr_num
                     from wqx."ORG_ADDRESS" org_address
                          join wqx."ADDRESS_TYPE" address_type
                            on org_address."ADDTYP_UID" = address_type."ADDTYP_UID"
                          left join wqx.country
                            on org_address."CNTRY_UID" = country.cntry_uid
                          left join wqx.state
                            on org_address."ST_UID" = state.st_uid
                          left join wqx.county
                            on org_address."CNTY_UID" = county.cnty_uid)
insert
  into org_data_swap_storet (data_source_id, data_source, organization_id, organization, organization_name,
                             organization_description, organization_type, tribal_code, electronic_address, telephonic, address_type_1,
                             address_text_1, supplemental_address_text_1, locality_name_1, postal_code_1,
                             country_code_1, state_code_1, county_code_1, address_type_2, address_text_2,
                             supplemental_address_text_2, locality_name_2, postal_code_2, country_code_2,
                             state_code_2, county_code_2, address_type_3, address_text_3,
                             supplemental_address_text_3, locality_name_3, postal_code_3, country_code_3,
                             state_code_3, county_code_3)
select 3 data_source_id,
       'STORET' data_source,
       organization."ORG_UID" organization_id,
       organization."ORG_ID" organization,
       organization."ORG_NAME" organization_name,
       organization."ORG_DESC" organization_description,
       organization."ORG_TYPE" organization_type,
       tribe."TRB_NAME" tribal_code,
       org_electronic_address.electronic_address,
       org_phone.telephonic,
       addresses1.address_type address_type_1,
       addresses1.address_text address_text_1,
       addresses1.supplemental_address_text supplemental_address_text_1,
       addresses1.locality_name locality_name_1,
       addresses1.postal_code postal_code_1,
       addresses1.country_code country_code_1,
       addresses1.state_code state_code_1,
       addresses1.county_code county_code_1,
       addresses2.address_type address_type_2,
       addresses2.address_text address_text_2,
       addresses2.supplemental_address_text supplemental_address_text_2,
       addresses2.locality_name locality_name_2,
       addresses2.postal_code postal_code_2,
       addresses2.country_code country_code_2,
       addresses2.state_code state_code_2,
       addresses2.county_code county_code_2,
       addresses3.address_type address_type_3,
       addresses3.address_text address_text_3,
       addresses3.supplemental_address_text supplemental_address_text_3,
       addresses3.locality_name locality_name_3,
       addresses3.postal_code postal_code_3,
       addresses3.country_code country_code_3,
       addresses3.state_code state_code_3,
       addresses3.county_code county_code_3
  from wqx."ORGANIZATION" organization
       left join wqx."TRIBE" tribe
         on organization."TRB_UID" = tribe."TRB_UID"
       left join (select "ORG_UID" org_uid,
                         string_agg("ORGEA_TEXT" || ' (' || "EATYP_NAME" || ')', ';' order by "ORGEA_UID") electronic_address
                    from wqx."ORG_ELECTRONIC_ADDRESS" org_electronic_address
                         join wqx."ELECTRONIC_ADDRESS_TYPE" electronic_address_type
                           on org_electronic_address."EATYP_UID" = electronic_address_type."EATYP_UID"
                      group by org_uid) org_electronic_address
         on organization."ORG_UID" = org_electronic_address.org_uid
       left join (select "ORG_UID" org_uid,
                         string_agg("ORGPH_NUM" ||
                                   case when "ORGPH_EXT" is not null then ' x' || "ORGPH_EXT" end || 
                                   ' (' || "PHTYP_NAME" || ')', ';'
                             order by "ORGPH_UID") telephonic
                    from wqx."ORG_PHONE" org_phone
                         join wqx."PHONE_TYPE" phone_type
                           on org_phone."PHTYP_UID" = phone_type."PHTYP_UID"
                      group by org_uid) org_phone
         on organization."ORG_UID" = org_phone.org_uid
       left join addresses addresses1
          on organization."ORG_UID" = addresses1.org_uid and
             1 = addresses1.addr_num
        left join addresses addresses2
          on organization."ORG_UID" = addresses2.org_uid and
             2 = addresses2.addr_num
        left join addresses addresses3
          on organization."ORG_UID" = addresses3.org_uid and
             3 = addresses3.addr_num
 where organization."ORG_UID" not between 2000 and 2999
