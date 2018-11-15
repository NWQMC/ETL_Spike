package gov.acwi.nwqmc.etl.result;

import java.io.IOException;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.util.FileCopyUtils;

import gov.acwi.nwqmc.etl.MapItemSqlParameterSourceProvider;

@Configuration
public class TransformWqpNemiEpaCrosswalk {

	@Autowired
	@Qualifier("wqpDataSource")
	DataSource wqpDataSource;
	@Autowired
	@Qualifier("nemiDataSource")
	DataSource nemiDataSource;
	@Autowired
	StepBuilderFactory stepBuilderFactory;
	@Value("classpath:sql/result/readWqpNemiEpaCrosswalk.sql")
	private Resource readerResource;
	@Value("classpath:sql/result/writeWqpNemiEpaCrosswalk.sql")
	private Resource writerResource;

	@Bean
	public JdbcCursorItemReader<Map<String, Object>> myItemReader() throws IOException {
		return new JdbcCursorItemReaderBuilder<Map<String, Object>>()
				.dataSource(nemiDataSource)
				.name("nemiWqpNemiEpaCrosswalk")
				.sql(new String(FileCopyUtils.copyToByteArray(readerResource.getInputStream())))
				.rowMapper(new ColumnMapRowMapper())
				.build();
	}

	@Bean
	public JdbcBatchItemWriter<Map<String, Object>> myWriter() throws IOException {
		return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
				.itemSqlParameterSourceProvider(new MapItemSqlParameterSourceProvider())
				.sql(new String(FileCopyUtils.copyToByteArray(writerResource.getInputStream())))
				.dataSource(wqpDataSource)
				.build();
	}

	@Bean
	public Step transformWqpNemiEpaCrosswalkStep() throws IOException {
		return stepBuilderFactory.get("transformWqpNemiEpaCrosswalkStep")
				.<Map<String, Object>, Map<String, Object>> chunk(1)
				.reader(myItemReader())
				.processor(new WqpNemiEpaCrosswalkItemProcessor())
				.writer(myWriter())
				.build();
	}
}
