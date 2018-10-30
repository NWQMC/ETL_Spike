package gov.acwi.nwqmc.etl.monitoringLocationObject;

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
public class TransformMonitoringLocationObjectWqx implements Tasklet {

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public TransformMonitoringLocationObjectWqx(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		jdbcTemplate.execute("insert /*+ append parallel(4) */\n" + 
				"  into station_object_swap_storet (data_source_id,\n" + 
				"                                   object_id,\n" + 
				"                                   data_source,\n" + 
				"                                   organization,\n" + 
				"                                   station_id,\n" + 
				"                                   site_id,\n" + 
				"                                   object_name,\n" + 
				"                                   object_type,\n" + 
				"                                   object_content)\n" + 
				"select '3' data_source_id,\n" + 
				"       attached_object.atobj_uid object_id,\n" + 
				"       'STORET' data_source,\n" + 
				"       organization.org_id organization,\n" + 
				"       attached_object.ref_uid station_id,\n" + 
				"       station.site_id,\n" + 
				"       attached_object.atobj_file_name object_name,\n" + 
				"       attached_object.atobj_type object_type,\n" + 
				"       attached_object.atobj_content object_content\n" + 
				"  from wqx.attached_object\n" + 
				"       join wqx.organization\n" + 
				"         on attached_object.org_uid = organization.org_uid\n" + 
				"       join station_swap_storet station\n" + 
				"         on attached_object.ref_uid = station.station_id\n" + 
				" where tbl_uid = 2");
		return RepeatStatus.FINISHED;
	}
}
