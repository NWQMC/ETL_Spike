package gov.acwi.nwqmc.etl.project;

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
public class TransformProjectWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformProjectWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into project_data_swap_storet (data_source_id,\n" + 
				"                                 project_id,\n" + 
				"                                 data_source,\n" + 
				"                                 organization,\n" + 
				"                                 organization_name,\n" + 
				"                                 project_identifier,\n" + 
				"                                 project_name,\n" + 
				"                                 description,\n" + 
				"                                 sampling_design_type_code,\n" + 
				"                                 qapp_approved_indicator,\n" + 
				"                                 qapp_approval_agency_name,\n" + 
				"                                 project_file_url,\n" + 
				"                                 monitoring_location_weight_url\n" + 
				"                                )\n" + 
				"select 3 data_source_id,\n" + 
				"       project.prj_uid project_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       organization.org_id organization,\n" + 
				"       organization.org_name organization_name,\n" + 
				"       project.prj_id project_identifier,\n" + 
				"       project.prj_name project_name,\n" + 
				"       project.prj_desc description,\n" + 
				"       sampling_design_type.sdtyp_desc sampling_design_type_code,\n" + 
				"       project.prj_qapp_approved_yn qapp_approved_indicator,\n" + 
				"       project.prj_qapp_approval_agency_name qapp_approval_agency_name,\n" + 
				"       case \n" + 
				"         when attached_object.has_blob is not null\n" + 
				"           then '/organizations/' || pkg_dynamic_list.url_escape(organization.org_id, 'true') || '/projects/' || pkg_dynamic_list.url_escape(project.prj_id, 'true') || '/files'\n" + 
				"         else null\n" + 
				"       end project_file_url,\n" + 
				"       case\n" + 
				"         when monitoring_location_weight.has_weight is not null\n" + 
				"           then '/organizations/' || pkg_dynamic_list.url_escape(organization.org_id, 'true') || '/projects/' || pkg_dynamic_list.url_escape(project.prj_id, 'true') || '/projectMonitoringLocationWeightings'\n" + 
				"         else null\n" + 
				"       end monitoring_location_weight_url\n" + 
				"  from wqx.project\n" + 
				"       join wqx.organization\n" + 
				"         on project.org_uid = organization.org_uid\n" + 
				"       left join wqx.sampling_design_type\n" + 
				"         on project.sdtyp_uid = sampling_design_type.sdtyp_uid\n" + 
				"       left join (select org_uid, ref_uid, count(*) has_blob\n" + 
				"                    from wqx.attached_object\n" + 
				"                   where 1 = tbl_uid\n" + 
				"                     group by org_uid, ref_uid) attached_object\n" + 
				"         on project.org_uid = attached_object.org_uid and\n" + 
				"            project.prj_uid = attached_object.ref_uid\n" + 
				"       left join (select prj_uid, count(*) has_weight\n" + 
				"                    from wqx.monitoring_location_weight\n" + 
				"                      group by prj_uid) monitoring_location_weight\n" + 
				"         on project.prj_uid = monitoring_location_weight.prj_uid");
		return RepeatStatus.FINISHED;
	}
}
