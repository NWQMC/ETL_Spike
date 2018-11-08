package gov.acwi.nwqmc.etl.result;

import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.LinkedCaseInsensitiveMap;

import gov.acwi.nwqmc.etl.Application;

@Configuration
public class Nemi {

	@Autowired
	@Qualifier("firstDataSource")
	DataSource firstDataSource;
	@Autowired
	@Qualifier("secondDataSource")
	DataSource secondDataSource;
	@Autowired
	StepBuilderFactory stepBuilderFactory;

	@Bean
	public JdbcCursorItemReader<Map<String, Object>> myItemReader() {
		return new JdbcCursorItemReaderBuilder<Map<String, Object>>()
				.dataSource(secondDataSource)
				.name("nemiWqpNemiEpaCrosswalk")
				.sql("select wqp_source, analytical_procedure_source, analytical_procedure_id, "
						+ "source_method_identifier, method_id, method_source, method_type"
						+ "  from wqp_nemi_epa_crosswalk")
				.rowMapper(new ColumnMapRowMapper())
				.build();
	}

	@Bean
	public JdbcBatchItemWriter<Map<String, Object>> myWriter() {
		return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
				.itemSqlParameterSourceProvider(new MapItemSqlParameterSourceProvider())
				.sql("insert into wqp_nemi_epa_crosswalk (wqp_source, analytical_procedure_source, analytical_procedure_id,"
						+ "source_method_identifier, method_id, method_source, method_type, nemi_url)"
						+ " VALUES (:WQP_SOURCE, :ANALYTICAL_PROCEDURE_SOURCE, :ANALYTICAL_PROCEDURE_ID,"
						+ ":SOURCE_METHOD_IDENTIFIER, :METHOD_ID, :METHOD_SOURCE, :METHOD_TYPE, :NEMI_URL)")
				.dataSource(firstDataSource)
				.build();
	}

	@Bean
	public Step transformWqpNemiEpaCrosswalkStep() {
		return stepBuilderFactory.get("transformWqpNemiEpaCrosswalkStep")
				.<Map<String, Object>, Map<String, Object>> chunk(1)
				.reader(myItemReader())
				.processor(new NemiItemProcessor())
				.writer(myWriter())
				.build();
	}

	public class MapItemSqlParameterSourceProvider implements ItemSqlParameterSourceProvider<Map<String, Object>> {
		public SqlParameterSource createSqlParameterSource(Map<String, Object> item) {
			return new MapSqlParameterSource(item);
		}
	}

	public class NemiItemProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {
		private final Logger LOG = LoggerFactory.getLogger(Application.class);
		@Override
		public Map<String, Object> process(final Map<String, Object> map) throws Exception {
			LinkedCaseInsensitiveMap<Object> newMap = new LinkedCaseInsensitiveMap<>();
			newMap.putAll(map);
			if (map.containsKey("method_id") && null != map.get("method_id") && map.containsKey("method_type")) {
				newMap.put("NEMI_URL", getNemiUrl(String.valueOf(map.get("method_id")), String.valueOf(map.get("method_type"))));
			} else {
				newMap.put("NEMI_URL", null);
			}
			LOG.info(newMap.toString());
			return newMap;
		}

		protected String getNemiUrl(String methodId, String methodType) {
			switch (methodType) {
			case "analytical":
				return "https://www.nemi.gov/methods/method_summary/" + methodId;
			case "statistical":
				return "https://www.nemi.gov/methods/sams_method_summary/" + methodId;
			default:
				return null;
			}
		}
	}
}
