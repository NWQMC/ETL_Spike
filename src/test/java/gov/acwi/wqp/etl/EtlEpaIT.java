package gov.acwi.wqp.etl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.dbFinalize.UpdateLastETLIT;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.test.context.TestPropertySource;

/*
Configure to create minimal partitions so that the partitioning and indexing happens fast for the test.
The test also checks that completion happens in under a minute, so the large number of partitions that would normally
be created take too long.
Setting the ETL_RUN_TIME ensures that table names are repeatable.
 */
@TestPropertySource(properties = {"ETL_RESULT_PARTITION_START_DATE=2015-01-01",
                                  "ETL_RESULT_PARTITION_ONE_YEAR_BREAK=2020-01-01",
                                  "ETL_RESULT_PARTITION_QUARTER_BREAK=2020-01-01",
                                  "ETL_RESULT_PARTITION_END_DATE=2020-01-01",
                                  "ETL_RUN_TIME=2021-01-01T10:15:30"})
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

	public static final String EXPECTED_INDEX_QUERY = "SELECT\n" +
			                                                  "\tc.relname AS tablename,\n" +
			                                                  "\ti.relname AS indexname,\n" +
			                                                  "\tpg_get_indexdef(i.oid) AS indexdef\n" +
			                                                  "FROM pg_index x\n" +
			                                                  "\tJOIN pg_class c ON c.oid = x.indrelid\n" +
			                                                  "\tJOIN pg_class i ON i.oid = x.indexrelid\n" +
			                                                  "\tLEFT JOIN pg_namespace n ON n.oid = c.relnamespace\n" +
			                                                  "\tLEFT JOIN pg_tablespace t ON t.oid = i.reltablespace\n" +
			                                                  "WHERE (c.relkind in ('r','m','p'))\n" +
			                                                  "\tAND i.relkind in ('i', 'I')\n" +
			                                                  "\tand i.relname not like '%pk'\n" +
			                                                  "\tand c.relname not like '%swap%'\n" +
			                                                  "\tand c.relname like '%storet%'\n" +
			                                                  "\tand c.relname not like '%_old'\n" +
			                                                  "order by indexname";




	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_RESULT_SWAP_TABLE_FLOW)
	private Flow setupResultSwapTableFlow;

	@BeforeEach
	public void setUp() throws Exception {

		//Need to drop the result swap table because it will have partitions with names that collide w/ newly created
		//partitions.  Normally the names are time unique, but that assumes runs are at least an hour apart.
		dropSwapTable();

		//Can't reuse that job, so create a new unique one for the actual test.
		baseSetup();
	}

	protected void dropSwapTable() throws Exception {


		//Going to use the testUtil to run this setup job, but will need to restore the original configured job, so save it.
		Job orgJob = jobLauncherTestUtils.getJob();

		Job dropResultTbl = jobBuilderFactory.get("setupResultSwapTableFlowTest")
				                     .start(setupResultSwapTableFlow)
				                     .build()
				                     .build();

		jobLauncherTestUtils.setJob(dropResultTbl);

		JobExecution jobExecution = jobLauncherTestUtils
				                            .launchStep("dropResultSwapTableStep", testJobParameters);

		assert(! jobExecution.getStatus().isUnsuccessful());

		//rest to original test job for actual testing
		jobLauncherTestUtils.setJob(orgJob);
	}


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

	//********************************************************************************************
	//Indexes - This hasn't been updated for the new list of queries on the partitioned tables yet
	//Once updated to PG12, do I need the updated query that is in here??
	@ExpectedDatabase(
			connection=CONNECTION_INFORMATION_SCHEMA,
			value="classpath:/testResult/wqp/installIndexes/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_INDEX,
			query=EXPECTED_INDEX_QUERY)

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

		System.out.println("EXPECTED_DATABASE_QUERY_TABLE: " + EXPECTED_DATABASE_QUERY_TABLE);
		System.out.println("Expected EXPECTED_DATABASE_QUERY_INDEX: " + EXPECTED_DATABASE_QUERY_INDEX);

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
