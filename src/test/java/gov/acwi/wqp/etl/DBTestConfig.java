package gov.acwi.wqp.etl;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.dbunit.ext.oracle.OracleDataTypeFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import com.github.springtestdbunit.bean.DatabaseConfigBean;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;

import liquibase.integration.spring.SpringLiquibase;
import oracle.jdbc.pool.OracleDataSource;

@TestConfiguration
public class DBTestConfig {

	@Value("${app.datasource.wqp.url}")
	private String datasourceUrl;

	@Value("${app.datasource.wqp.username}")
	private String datasourceUsername;

	@Value("${app.datasource.wqp.password}")
	private String datasourcePassword;

	@Bean
	public DataSource wqpDataSourceDbUnit() throws SQLException {
		OracleDataSource ds = new OracleDataSource();
		ds.setURL(datasourceUrl);
		ds.setUser(datasourceUsername);
		ds.setPassword(datasourcePassword);
		return ds;
	};

	@Autowired
	@Qualifier("nemiDataSource")
	DataSource nemiDataSource;

//	@Bean
//	@ConfigurationProperties("app.datasource.wqx")
//	public DataSourceProperties wqxDataSourceProperties() {
//		return new DataSourceProperties();
//	}
//
//	@Bean
//	@ConfigurationProperties("app.datasource.wqx")
//	public DataSource wqxDataSource() {
//		return wqxDataSourceProperties().initializeDataSourceBuilder().build();
//	}
//
//	@Bean
//	@ConfigurationProperties(prefix = "app.datasource.wqx.liquibase")
//	public LiquibaseProperties wqxLiquibaseProperties() {
//		return new LiquibaseProperties();
//	}

	@Bean
	@ConfigurationProperties(prefix = "app.datasource.nemi.liquibase")
	public LiquibaseProperties nemiLiquibaseProperties() {
		return new LiquibaseProperties();
	}

//	@Bean(name = "liquibase")
//	public SpringLiquibase wqxLiquibase(@Qualifier("wqxLiquibaseProperties") LiquibaseProperties liquibaseProperties) {
//		return springLiquibase(wqxDataSource(), wqxLiquibaseProperties());
//	}

	@Bean(name = "liquibase")
//	@Bean(name = "nemiLiquibase")
	public SpringLiquibase nemiLiquibase(@Qualifier("nemiLiquibaseProperties") LiquibaseProperties liquibaseProperties) {
		return springLiquibase(nemiDataSource, nemiLiquibaseProperties());
	}

	private static SpringLiquibase springLiquibase(DataSource dataSource, LiquibaseProperties properties) {
		SpringLiquibase liquibase = new SpringLiquibase();
		liquibase.setDataSource(dataSource);
		liquibase.setChangeLog(properties.getChangeLog());
		liquibase.setContexts(properties.getContexts());
		liquibase.setDefaultSchema(properties.getDefaultSchema());
		liquibase.setDropFirst(properties.isDropFirst());
		liquibase.setShouldRun(properties.isEnabled());
		liquibase.setLabels(properties.getLabels());
		liquibase.setChangeLogParameters(properties.getParameters());
		liquibase.setRollbackFile(properties.getRollbackFile());
		return liquibase;
	}

	//Beans to support DBunit for unit testing with Oracle.
	@Bean
	public DatabaseConfigBean dbUnitDatabaseConfig() {
		DatabaseConfigBean dbUnitDbConfig = new DatabaseConfigBean();
		dbUnitDbConfig.setDatatypeFactory(new OracleDataTypeFactory());
		dbUnitDbConfig.setSkipOracleRecyclebinTables(true);
		dbUnitDbConfig.setQualifiedTableNames(false);
		return dbUnitDbConfig;
	}

	@Bean
	public DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection() throws SQLException {
		DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection = new DatabaseDataSourceConnectionFactoryBean();
		dbUnitDatabaseConnection.setDatabaseConfig(dbUnitDatabaseConfig());
		dbUnitDatabaseConnection.setDataSource(wqpDataSourceDbUnit());
		dbUnitDatabaseConnection.setSchema("WQP_CORE");
		return dbUnitDatabaseConnection;
	}

}
