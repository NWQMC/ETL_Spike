package gov.acwi.wqp.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.dbFinalize.UpdateLastETLIT;

public class EtlEpaIT extends WqxBaseFlowIT {

	public static final String EXPECTED_DATABASE_TABLE_STATION_SUM = "station_sum_storet";
	public static final String EXPECTED_DATABASE_QUERY_STATION_SUM = BASE_EXPECTED_DATABASE_QUERY_STATION_SUM + EXPECTED_DATABASE_TABLE_STATION_SUM;

	public static final String EXPECTED_DATABASE_QUERY_TABLE = BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE_LIKE
			+ "'%storet%' and table_name not like '%swap%' and table_schema = 'wqp'";
	public static final String EXPECTED_DATABASE_QUERY_INDEX = BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX_LIKE
			+ "'%storet' and tablename not like '%swap%'";

	public static final String EXPECTED_DATABASE_QUERY_ANALYZE = BASE_EXPECTED_DATABASE_QUERY_ANALYZE_BARE
			+ "where relname like '%_storet' and relname not like '%swap%' and relname not like '%object%'";

	public static final String EXPECTED_DATABASE_QUERY_PRIMARY_KEY = BASE_EXPECTED_DATABASE_QUERY_PRIMARY_KEY
			+ " like '%_storet'";

	public static final String EXPECTED_DATABASE_QUERY_FOREIGN_KEY = BASE_EXPECTED_DATABASE_QUERY_FOREIGN_KEY
			+ " like '%_storet'";

	@Test
	//Geospatial and lastEtl from wqp-etl-core
	@DatabaseSetup(connection=CONNECTION_NWIS, value="classpath:/testData/nwis/country/country.xml")
	@DatabaseSetup(connection=CONNECTION_NWIS, value="classpath:/testData/nwis/state/state.xml")
	@DatabaseSetup(connection=CONNECTION_NWIS, value="classpath:/testData/nwis/county/county.xml")

	@DatabaseSetup(value="classpath:/testData/wqp/lastEtl/lastEtl.xml")
	@DatabaseSetup(value="classpath:/testData/wqp/huc12nometa/")
	@DatabaseSetup(value="classpath:/testData/wqp/qwportalSummary/")
	@DatabaseSetup(value="classpath:/testData/wqp/tl2019UsCountyGeopkg/")

	@DatabaseSetup(
			connection=CONNECTION_WQX,
			value = "classpath:/testData/wqx/dqlHierarchy/csv/")
	@DatabaseSetup(
			connection=CONNECTION_WQX,
			value="classpath:/testData/wqx/monitoringLocationLocal/monitoring_location_local.xml"
			)

	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/storetw/orgDataNoSource/csv/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/storetw/projectDataNoSource/csv/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/storetw/stationNoSource/csv/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/storetw/activityNoSource/csv/"
			)
	@DatabaseSetup(
			connection=CONNECTION_STORETW,
			value="classpath:/testData/storetw/resultNoSource/csv/"
			)

	@DatabaseSetup(
			connection=CONNECTION_WQX_DUMP,
			value="classpath:/testData/wqxDump/csv/"
	)

	//Tables
	@ExpectedDatabase(
			connection=CONNECTION_INFORMATION_SCHEMA,
			value="classpath:/testResult/wqp/installTables/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_TABLE,
			query=EXPECTED_DATABASE_QUERY_TABLE)

	//Indexes
	@ExpectedDatabase(
			connection=CONNECTION_INFORMATION_SCHEMA,
			value="classpath:/testResult/wqp/installIndexes/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_INDEX,
			query=EXPECTED_DATABASE_QUERY_INDEX)

	//Analyzed
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/installAnalyzed/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_ANALYZE,
			query=EXPECTED_DATABASE_QUERY_ANALYZE)

	//Primary Keys
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/primaryKey/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_PRIMARY_KEY,
			query=EXPECTED_DATABASE_QUERY_PRIMARY_KEY)

	//Foreign Keys
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/foreignKey/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_FOREIGN_KEY,
			query=EXPECTED_DATABASE_QUERY_FOREIGN_KEY)

	//Storet Base Data
	//TODO - WQP-1415
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/tableData/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	//TODO - WQP-1458
//	@ExpectedDatabase(value="classpath:/testResult/wqp/biologicalHabitatMetric.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	//TODO - WQP-1425
//	@ExpectedDatabase(value="classpath:/testResult/wqp/activityMetric.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	//TODO - WQP-1428
//	@ExpectedDatabase(value="classpath:/testResult/wqp/resDetectQntLimit.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	//TODO - WQP-1429
//	@ExpectedDatabase(value="classpath:/testResult/wqp/projectMLWeighting.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)

	//Summaries Data
	//TODO - WQP-1430
//	@ExpectedDatabase(value="classpath:/testResult/wqp/activitySum.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/resultSum.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/orgGrouping.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/mlGrouping.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/organizationSum.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(
//			value="classpath:/testResult/wqp/monitoringLocationSum.xml",
//			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
//			table=EXPECTED_DATABASE_TABLE_STATION_SUM,
//			query=EXPECTED_DATABASE_QUERY_STATION_SUM)
	@ExpectedDatabase(value = "classpath:/testResult/wqp/qwportalSummary.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)

//	//Codes Data
	//TODO - WQP-1431
//	@ExpectedDatabase(value="classpath:/testResult/wqp/assemblage.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/characteristicName.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/characteristicType.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/country.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/county.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/monitoringLoc.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/organization.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/project.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/projectDim.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/sampleMedia.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/siteType.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/state.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
//	@ExpectedDatabase(value="classpath:/testResult/wqp/taxaName.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)

	@ExpectedDatabase(
			value="classpath:/testResult/wqp/lastEtl.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=UpdateLastETLIT.TABLE_NAME_LAST_ETL,
			query=UpdateLastETLIT.EXPECTED_DATABASE_QUERY_LAST_ETL)

	public void endToEndTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
