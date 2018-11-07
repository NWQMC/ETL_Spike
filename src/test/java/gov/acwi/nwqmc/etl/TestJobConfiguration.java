package gov.acwi.nwqmc.etl;

import org.springframework.boot.autoconfigure.AutoConfigurationExcludeFilter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

@TestConfiguration
@EnableAutoConfiguration
@ComponentScan(excludeFilters = {
		@Filter(type = FilterType.CUSTOM, classes = TypeExcludeFilter.class),
		@Filter(type = FilterType.CUSTOM, classes = AutoConfigurationExcludeFilter.class),
		@Filter(type = FilterType.REGEX, pattern = "gov.acwi.nwqmc.etl.Application"),
		@Filter(type = FilterType.REGEX, pattern = "gov.acwi.nwqmc.etl.BatchConfiguration")})
@Import(DBTestConfig.class)
public class TestJobConfiguration {

}
