package gov.acwi.wqp.etl;

import org.junit.Before;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import com.github.springtestdbunit.annotation.DbUnitConfiguration;

@Import({WqxDBTestConfig.class})
@DbUnitConfiguration(
		databaseConnection={
				BaseFlowIT.CONNECTION_WQP,
				WqxBaseFlowIT.CONNECTION_WQX,
				BaseFlowIT.CONNECTION_NWIS,
				BaseFlowIT.CONNECTION_INFORMATION_SCHEMA
				},
		dataSetLoader=FileSensingDataSetLoader.class
)
public abstract class WqxBaseFlowIT extends BaseFlowIT {

	public static final String CONNECTION_WQX = "wqx";

	@Autowired
	private ConfigurationService configurationService;

	@Before
	@Override
	public void baseSetup() {
		testJobParameters= new JobParametersBuilder()
				.addJobParameters(jobLauncherTestUtils.getUniqueJobParameters())
				.addString(EtlConstantUtils.JOB_PARM_DATA_SOURCE_ID, configurationService.getEtlDataSourceId().toString(), true)
				.addString(EtlConstantUtils.JOB_PARM_DATA_SOURCE, configurationService.getEtlDataSource().toLowerCase(), true)
				.addString(EtlConstantUtils.JOB_PARM_WQP_SCHEMA, configurationService.getWqpSchemaName(), false)
				.addString(EtlConstantUtils.JOB_PARM_GEO_SCHEMA, configurationService.getGeoSchemaName(), false)
				.toJobParameters();
	}


}
