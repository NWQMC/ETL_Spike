package gov.acwi.wqp.etl;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import com.github.springtestdbunit.bean.DatabaseConfigBean;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;

import liquibase.integration.spring.SpringLiquibase;

@TestConfiguration
public class WqxDBTestConfig {

	@Autowired
	@Qualifier(Application.DATASOURCE_WQX_QUALIFIER)
	private DataSource dataSourceWqx;

	@Autowired
	private DatabaseConfigBean dbUnitDatabaseConfig;

	@Autowired
	@Qualifier("liquibasePropertiesEpa")
	private LiquibaseProperties liquibasePropertiesEpa;

	@Bean
	public SpringLiquibase liquibaseTestEpa() {
		return instantiateSpringLiquibase(dataSourceWqx, liquibasePropertiesEpa);
	}

	private SpringLiquibase instantiateSpringLiquibase(DataSource dataSource, LiquibaseProperties liquibaseProperties) {
		SpringLiquibase springLiquibase = new SpringLiquibase();
		springLiquibase.setDataSource(dataSource);
		springLiquibase.setChangeLog("db/wqx/changelog/db.changelog-master.yaml");
		springLiquibase.setChangeLogParameters(liquibaseProperties.getParameters());
		springLiquibase.setLiquibaseSchema(liquibaseProperties.getLiquibaseSchema());
		return springLiquibase;
	}

	@Bean
	public DatabaseDataSourceConnectionFactoryBean wqx() throws SQLException {
		DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection = new DatabaseDataSourceConnectionFactoryBean();
		dbUnitDatabaseConnection.setDatabaseConfig(dbUnitDatabaseConfig);
		dbUnitDatabaseConnection.setDataSource(dataSourceWqx);
		dbUnitDatabaseConnection.setSchema("wqx");
		return dbUnitDatabaseConnection;
	}
}
