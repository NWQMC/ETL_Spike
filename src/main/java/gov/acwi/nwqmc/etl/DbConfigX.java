package gov.acwi.nwqmc.etl;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class DbConfigX {

	@Bean
	@Primary
	@ConfigurationProperties("app.datasource.wqp")
	public DataSourceProperties firstDataSourceProperties() {
		return new DataSourceProperties();
	}

	@Bean
	@Primary
	@ConfigurationProperties("app.datasource.wqp")
	public DataSource firstDataSource() {
		return firstDataSourceProperties().initializeDataSourceBuilder().build();
	}

	@Bean
	@ConfigurationProperties("app.datasource.nemi")
	public DataSourceProperties secondDataSourceProperties() {
		return new DataSourceProperties();
	}

	@Bean
	@ConfigurationProperties("app.datasource.nemi")
	public DataSource secondDataSource() {
		return secondDataSourceProperties().initializeDataSourceBuilder().build();
	}
}
