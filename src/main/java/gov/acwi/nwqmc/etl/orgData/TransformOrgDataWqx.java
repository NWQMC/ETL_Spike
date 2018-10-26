package gov.acwi.nwqmc.etl.orgData;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@StepScope
public class TransformOrgDataWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformOrgDataWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into org_data_swap_storet (data_source_id, data_source, organization_id, organization, organization_name,\n" + 
				"                             organization_description, organization_type, tribal_code, electronic_address, telephonic, address_type_1,\n" + 
				"                             address_text_1, supplemental_address_text_1, locality_name_1, postal_code_1,\n" + 
				"                             country_code_1, state_code_1, county_code_1, address_type_2, address_text_2,\n" + 
				"                             supplemental_address_text_2, locality_name_2, postal_code_2, country_code_2,\n" + 
				"                             state_code_2, county_code_2, address_type_3, address_text_3,\n" + 
				"                             supplemental_address_text_3, locality_name_3, postal_code_3, country_code_3,\n" + 
				"                             state_code_3, county_code_3)\n" + 
				"select /*+ parallel(4) */ \n" + 
				"       3 data_source_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       organization.org_uid organization_id,\n" + 
				"       organization.org_id organization,\n" + 
				"       organization.org_name organization_name,\n" + 
				"       organization.org_desc organization_description,\n" + 
				"       organization.org_type organization_type,\n" + 
				"       tribe.trb_name tribal_code,\n" + 
				"       org_electronic_address.electronic_address,\n" + 
				"       org_phone.telephonic,\n" + 
				"       org_address.a_addtyp_name address_type_1,\n" + 
				"       org_address.a_orgadd_address address_text_1,\n" + 
				"       org_address.a_orgadd_address_supplemental supplemental_address_text_1,\n" + 
				"       org_address.a_orgadd_locality_name locality_name_1,\n" + 
				"       org_address.a_orgadd_postal_cd postal_code_1,\n" + 
				"       org_address.a_cntry_cd country_code_1,\n" + 
				"       org_address.a_st_cd state_code_1,\n" + 
				"       org_address.a_cnty_fips_cd county_code_1,\n" + 
				"       org_address.b_addtyp_name address_type_2,\n" + 
				"       org_address.b_orgadd_address address_text_2,\n" + 
				"       org_address.b_orgadd_address_supplemental supplemental_address_text_2,\n" + 
				"       org_address.b_orgadd_locality_name locality_name_2,\n" + 
				"       org_address.b_orgadd_postal_cd postal_code_2,\n" + 
				"       org_address.b_cntry_cd country_code_2,\n" + 
				"       org_address.b_st_cd state_code_2,\n" + 
				"       org_address.b_cnty_fips_cd county_code_2,\n" + 
				"       org_address.c_addtyp_name address_type_3,\n" + 
				"       org_address.c_orgadd_address address_text_3,\n" + 
				"       org_address.c_orgadd_address_supplemental supplemental_address_text_3,\n" + 
				"       org_address.c_orgadd_locality_name locality_name_3,\n" + 
				"       org_address.c_orgadd_postal_cd postal_code_3,\n" + 
				"       org_address.c_cntry_cd country_code_3,\n" + 
				"       org_address.c_st_cd state_code_3,\n" + 
				"       org_address.c_cnty_fips_cd county_code_3\n" + 
				"  from wqx.organization\n" + 
				"       left join wqx.tribe\n" + 
				"         on organization.trb_uid = tribe.trb_uid\n" + 
				"       left join (select org_uid,\n" + 
				"                         listagg(orgea_text || ' (' || eatyp_name || ')', ';') within group (order by orgea_uid) electronic_address\n" + 
				"                    from wqx.org_electronic_address\n" + 
				"                         join wqx.electronic_address_type\n" + 
				"                           on org_electronic_address.eatyp_uid = electronic_address_type.eatyp_uid\n" + 
				"                      group by org_uid) org_electronic_address\n" + 
				"         on organization.org_uid = org_electronic_address.org_uid\n" + 
				"       left join (select org_uid,\n" + 
				"                         listagg(orgph_num ||\n" + 
				"                                   case when orgph_ext is not null then ' x' || orgph_ext end || \n" + 
				"                                   ' (' || phtyp_name || ')', ';')\n" + 
				"                             within group (order by orgph_uid) telephonic\n" + 
				"                    from wqx.org_phone\n" + 
				"                         join wqx.phone_type\n" + 
				"                           on org_phone.phtyp_uid = phone_type.phtyp_uid\n" + 
				"                      group by org_uid) org_phone\n" + 
				"         on organization.org_uid = org_phone.org_uid\n" + 
				"       left join (select *\n" + 
				"                    from (select org_address.org_uid,\n" + 
				"                                 address_type.addtyp_name,\n" + 
				"                                 org_address.orgadd_address,\n" + 
				"                                 org_address.orgadd_address_supplemental,\n" + 
				"                                 org_address.orgadd_locality_name,\n" + 
				"                                 org_address.orgadd_postal_cd,\n" + 
				"                                 country.cntry_cd,\n" + 
				"                                 state.st_cd,\n" + 
				"                                 county.cnty_fips_cd,\n" + 
				"                                 row_number() over (partition by org_address.org_uid order by org_address.orgadd_uid) addr_num\n" + 
				"                            from wqx.org_address\n" + 
				"                                 join wqx.address_type\n" + 
				"                                   on org_address.addtyp_uid = address_type.addtyp_uid\n" + 
				"                                 left join wqx.country\n" + 
				"                                   on org_address.cntry_uid = country.cntry_uid\n" + 
				"                                 left join wqx.state\n" + 
				"                                   on org_address.st_uid = state.st_uid\n" + 
				"                                 left join wqx.county\n" + 
				"                                   on org_address.cnty_uid = county.cnty_uid)\n" + 
				"                   pivot (min(addtyp_name) addtyp_name,\n" + 
				"                          min(orgadd_address) orgadd_address,\n" + 
				"                          min(orgadd_address_supplemental) orgadd_address_supplemental,\n" + 
				"                          min(orgadd_locality_name) orgadd_locality_name,\n" + 
				"                          min(orgadd_postal_cd) orgadd_postal_cd,\n" + 
				"                          min(cntry_cd) cntry_cd,\n" + 
				"                          min(st_cd) st_cd,\n" + 
				"                          min(cnty_fips_cd) cnty_fips_cd\n" + 
				"                           for (addr_num) in (1 a, 2 b, 3 c)\n" + 
				"                         )\n" + 
				"                 ) org_address\n" + 
				"         on organization.org_uid = org_address.org_uid\n" + 
				" where organization.org_uid not between 2000 and 2999");
		return RepeatStatus.FINISHED;
	}
}
