package gov.acwi.wqp.etl.monitoringLocation;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MonitoringLocationTransformation {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("purgeWqxMonitoringLocationLocalWqx")
	private Tasklet purgeWqxMonitoringLocationLocalWqx;

	@Autowired
	@Qualifier("updateWqxMonitoringLocationLocalWqx")
	private Tasklet updateWqxMonitoringLocationLocalWqx;

	@Autowired
	@Qualifier("purgeWqxMonitoringLocationLocalStoretw")
	private Tasklet purgeWqxMonitoringLocationLocalStoretw;

	@Autowired
	@Qualifier("updateWqxMonitoringLocationLocalStoretw")
	private Tasklet updateWqxMonitoringLocationLocalStoretw;

	@Autowired
	@Qualifier("calculateHuc")
	private Tasklet calculateHuc;

	@Autowired
	@Qualifier("calculateGeopoliticalData")
	private Tasklet calculateGeopoliticalData;

	@Autowired
	@Qualifier("dropMonitoringLocationIndexes")
	private Tasklet dropMonitoringLocationIndexes;

	@Autowired
	@Qualifier("truncateMonitoringLocation")
	private Tasklet truncateMonitoringLocation;

	@Autowired
	@Qualifier("transformMonitoringLocationWqx")
	private Tasklet transformMonitoringLocationWqx;

	@Autowired
	@Qualifier("transformMonitoringLocationStoretw")
	private Tasklet transformMonitoringLocationStoretw;

	@Autowired
	@Qualifier("buildMonitoringLocationIndexes")
	private Tasklet buildMonitoringLocationIndexes;

	@Bean
	public Flow monitoringLocationFlow() {
		return new FlowBuilder<SimpleFlow>("monitoringLocationFlow")
				.start(purgeWqxMonitoringLocationLocalWqxStep())
				.next(updateWqxMonitoringLocationLocalWqxStep())
				.next(purgeWqxMonitoringLocationLocalStoretwStep())
				.next(updateWqxMonitoringLocationLocalStoretwStep())
				.next(calculateHucStep())
				.next(calculateGeopoliticalDataStep())
				.next(dropMonitoringLocationIndexesStep())
				.next(truncateMonitoringLocationStep())
				.next(transformMonitoringLocationWqxStep())
				.next(transformMonitoringLocationStoretwStep())
				.next(buildMonitoringLocationIndexesStep())
				.build();
	}

	@Bean
	public Step purgeWqxMonitoringLocationLocalWqxStep() {
		return stepBuilderFactory.get("purgeWqxMonitoringLocationLocalWqxStep")
				.tasklet(purgeWqxMonitoringLocationLocalWqx)
				.build();
	}

	@Bean
	public Step updateWqxMonitoringLocationLocalWqxStep() {
		return stepBuilderFactory.get("updateWqxMonitoringLocationLocalWqxStep")
				.tasklet(updateWqxMonitoringLocationLocalWqx)
				.build();
	}

	@Bean
	public Step purgeWqxMonitoringLocationLocalStoretwStep() {
		return stepBuilderFactory.get("purgeWqxMonitoringLocationLocalStoretwStep")
				.tasklet(purgeWqxMonitoringLocationLocalStoretw)
				.build();
	}

	@Bean
	public Step updateWqxMonitoringLocationLocalStoretwStep() {
		return stepBuilderFactory.get("updateWqxMonitoringLocationLocalStoretwStep")
				.tasklet(updateWqxMonitoringLocationLocalStoretw)
				.build();
	}

	@Bean
	public Step calculateHucStep() {
		return stepBuilderFactory.get("calculateHucStep")
				.tasklet(calculateHuc)
				.build();
	}

	@Bean
	public Step calculateGeopoliticalDataStep() {
		return stepBuilderFactory.get("calculateGeopoliticalDataStep")
				.tasklet(calculateGeopoliticalData)
				.build();
	}

	@Bean
	public Step dropMonitoringLocationIndexesStep() {
		return stepBuilderFactory.get("dropMonitoringLocationIndexesStep")
				.tasklet(dropMonitoringLocationIndexes)
				.build();
	}

	@Bean
	public Step truncateMonitoringLocationStep() {
		return stepBuilderFactory.get("truncateMonitoringLocationStep")
				.tasklet(truncateMonitoringLocation)
				.build();
	}

	@Bean
	public Step transformMonitoringLocationWqxStep() {
		return stepBuilderFactory.get("transformMonitoringLocationWqxStep")
				.tasklet(transformMonitoringLocationWqx)
				.build();
	}

	@Bean
	public Step transformMonitoringLocationStoretwStep() {
		return stepBuilderFactory.get("transformMonitoringLocationStoretwStep")
				.tasklet(transformMonitoringLocationStoretw)
				.build();
	}

	@Bean
	public Step buildMonitoringLocationIndexesStep() {
		return stepBuilderFactory.get("buildMonitoringLocationIndexesStep")
				.tasklet(buildMonitoringLocationIndexes)
				.build();
	}
}
