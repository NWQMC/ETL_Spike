package gov.acwi.wqp.etl;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import com.github.springtestdbunit.bean.DatabaseConfigBean;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;

@TestConfiguration
public class WqxDBTestConfig {

	@Autowired
	@Qualifier(Application.DATASOURCE_WQX_QUALIFIER)
	private DataSource dataSourceWqx;

	@Autowired
	private DatabaseConfigBean dbUnitDatabaseConfig;

	@Bean
	public DatabaseDataSourceConnectionFactoryBean wqx() throws SQLException {
		//Currently the WQX tables from EPA are in upper case.
		dbUnitDatabaseConfig.setCaseSensitiveTableNames(true);
		//And need to be enclosed in quotes for the delete portion of the database setup.
		dbUnitDatabaseConfig.setEscapePattern("\"?\"");
		DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection = new DatabaseDataSourceConnectionFactoryBean();
		dbUnitDatabaseConnection.setDatabaseConfig(dbUnitDatabaseConfig);
		dbUnitDatabaseConnection.setDataSource(dataSourceWqx);
		dbUnitDatabaseConnection.setSchema("wqx");
		return dbUnitDatabaseConnection;
	}

	@Bean
	public DatabaseDataSourceConnectionFactoryBean wqxDump() throws SQLException {
		//Currently the WQX tables from EPA are in upper case.
		dbUnitDatabaseConfig.setCaseSensitiveTableNames(true);
		//And need to be enclosed in quotes for the delete portion of the database setup.
		dbUnitDatabaseConfig.setEscapePattern("\"?\"");
		DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection = new DatabaseDataSourceConnectionFactoryBean();
		dbUnitDatabaseConnection.setDatabaseConfig(dbUnitDatabaseConfig);
		dbUnitDatabaseConnection.setDataSource(dataSourceWqx);
		dbUnitDatabaseConnection.setSchema("wqx_dump");
		return dbUnitDatabaseConnection;
	}

	@Bean
	public DatabaseDataSourceConnectionFactoryBean storetw() throws SQLException {
		//Currently the WQX tables from EPA are in upper case.
		dbUnitDatabaseConfig.setCaseSensitiveTableNames(true);
		//And need to be enclosed in quotes for the delete portion of the database setup.
		dbUnitDatabaseConfig.setEscapePattern("\"?\"");
		DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection = new DatabaseDataSourceConnectionFactoryBean();
		dbUnitDatabaseConnection.setDatabaseConfig(dbUnitDatabaseConfig);
		dbUnitDatabaseConnection.setDataSource(dataSourceWqx);
		dbUnitDatabaseConnection.setSchema("storetw");
		return dbUnitDatabaseConnection;
	}
}
