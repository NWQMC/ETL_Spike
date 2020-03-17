package gov.acwi.wqp.etl;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;

import liquibase.integration.spring.SpringLiquibase;

@Configuration
public class DbConfig {

	@Bean
	@ConfigurationProperties(prefix=EtlConstantUtils.SPRING_DATASOURCE_WQP)
	@Primary
	@Profile("default")
	public DataSourceProperties dataSourcePropertiesWqp() {
		return new DataSourceProperties();
	}

	@Bean
	@Primary
	@Profile("default")
	public DataSource dataSourceWqp() {
		return dataSourcePropertiesWqp().initializeDataSourceBuilder().build();
	}

	@Bean
	@Primary
	public JdbcTemplate jdbcTemplateWqp() {
		return new JdbcTemplate(dataSourceWqp());
	}

	@Bean
	@ConfigurationProperties(prefix=EtlConstantUtils.SPRING_DATASOURCE_WQX)
	public DataSourceProperties dataSourcePropertiesWqx() {
		return new DataSourceProperties();
	}

	@Bean
	public DataSource dataSourceWqx() {
		return dataSourcePropertiesWqx().initializeDataSourceBuilder().build();
	}

	@Bean
	public JdbcTemplate jdbcTemplateWqx() {
		return new JdbcTemplate(dataSourceWqx());
	}

	//TODO - WQP-1476
//	@Bean
//	@ConfigurationProperties(prefix="spring.datasource-nemi")
//	public DataSourceProperties dataSourcePropertiesNemi() {
//		return new DataSourceProperties();
//	}
//
//	@Bean
//	public DataSource dataSourceNemi() {
//		return dataSourcePropertiesNemi().initializeDataSourceBuilder().build();
//	}

	@Bean
	@ConfigurationProperties(prefix = "spring.liquibase")
	public LiquibaseProperties primaryLiquibaseProperties() {
		return new LiquibaseProperties();
	}

	@Bean
	public SpringLiquibase liquibase() {
		return instantiateSpringLiquibase(dataSourceWqp(), primaryLiquibaseProperties());
	}

	@Bean
	@ConfigurationProperties(prefix = "spring.datasource-wqx.liquibase")
	public LiquibaseProperties liquibasePropertiesEpa() {
		return new LiquibaseProperties();
	}

	@Bean
	public SpringLiquibase liquibaseEpa() {
		return instantiateSpringLiquibase(dataSourceWqx(), liquibasePropertiesEpa());
	}

	private SpringLiquibase instantiateSpringLiquibase(DataSource dataSource, LiquibaseProperties liquibaseProperties) {
		SpringLiquibase springLiquibase = new SpringLiquibase();
		springLiquibase.setDataSource(dataSource);
		springLiquibase.setChangeLog(liquibaseProperties.getChangeLog());
		springLiquibase.setChangeLogParameters(liquibaseProperties.getParameters());
		springLiquibase.setLiquibaseSchema(liquibaseProperties.getLiquibaseSchema());
		return springLiquibase;
	}
}
