package gov.acwi.nwqmc.etl.monitoringLocation;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class MonitoringLocationTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("purgeWqxMonitoringLocationLocalWqx")
	public Tasklet purgeWqxMonitoringLocationLocalWqx;

	@Autowired
	@Qualifier("updateWqxMonitoringLocationLocalWqx")
	public Tasklet updateWqxMonitoringLocationLocalWqx;

	@Autowired
	@Qualifier("purgeWqxMonitoringLocationLocalStoretw")
	public Tasklet purgeWqxMonitoringLocationLocalStoretw;

	@Autowired
	@Qualifier("updateWqxMonitoringLocationLocalStoretw")
	public Tasklet updateWqxMonitoringLocationLocalStoretw;

	@Autowired
	@Qualifier("calculateHuc")
	public Tasklet calculateHuc;

	@Autowired
	@Qualifier("calculateGeopoliticalData")
	public Tasklet calculateGeopoliticalData;

	@Autowired
	@Qualifier("dropMonitoringLocationIndexes")
	public Tasklet dropMonitoringLocationIndexes;

	@Autowired
	@Qualifier("truncateMonitoringLocation")
	public Tasklet truncateMonitoringLocation;

	@Autowired
	@Qualifier("transformMonitoringLocationWqx")
	public Tasklet transformMonitoringLocationWqx;

	@Autowired
	@Qualifier("transformMonitoringLocationStoretw")
	public Tasklet transformMonitoringLocationStoretw;

	@Autowired
	@Qualifier("buildMonitoringLocationIndexes")
	public Tasklet buildMonitoringLocationIndexes;

	public SimpleJobBuilder build(SimpleJobBuilder job) {
		return job.next(purgeWqxMonitoringLocationLocalWqx())
				.next(updateWqxMonitoringLocationLocalWqx())
				.next(purgeWqxMonitoringLocationLocalStoretw())
				.next(updateWqxMonitoringLocationLocalStoretw())
				.next(calculateHuc())
				.next(calculateGeopoliticalData())
				.next(dropMonitoringLocationIndexes())
				.next(truncateMonitoringLocation())
				.next(transformMonitoringLocationWqx())
				.next(transformMonitoringLocationStoretw())
				.next(buildMonitoringLocationIndexes());
	}

	public Step purgeWqxMonitoringLocationLocalWqx() {
		return stepBuilderFactory.get("purgeWqxMonitoringLocationLocalWqx")
				.tasklet(purgeWqxMonitoringLocationLocalWqx)
				.build();
	}

	public Step updateWqxMonitoringLocationLocalWqx() {
		return stepBuilderFactory.get("updateWqxMonitoringLocationLocalWqx")
				.tasklet(updateWqxMonitoringLocationLocalWqx)
				.build();
	}

	public Step purgeWqxMonitoringLocationLocalStoretw() {
		return stepBuilderFactory.get("purgeWqxMonitoringLocationLocalStoretw")
				.tasklet(purgeWqxMonitoringLocationLocalStoretw)
				.build();
	}

	public Step updateWqxMonitoringLocationLocalStoretw() {
		return stepBuilderFactory.get("updateWqxMonitoringLocationLocalStoretw")
				.tasklet(updateWqxMonitoringLocationLocalStoretw)
				.build();
	}

	public Step calculateHuc() {
		return stepBuilderFactory.get("calculateHuc")
				.tasklet(calculateHuc)
				.build();
	}

	public Step calculateGeopoliticalData() {
		return stepBuilderFactory.get("calculateGeopoliticalData")
				.tasklet(calculateGeopoliticalData)
				.build();
	}

	public Step dropMonitoringLocationIndexes() {
		return stepBuilderFactory.get("dropMonitoringLocationIndexes")
				.tasklet(dropMonitoringLocationIndexes)
				.build();
	}

	public Step truncateMonitoringLocation() {
		return stepBuilderFactory.get("truncateMonitoringLocation")
				.tasklet(truncateMonitoringLocation)
				.build();
	}

	public Step transformMonitoringLocationWqx() {
		return stepBuilderFactory.get("transformMonitoringLocationWqx")
				.tasklet(transformMonitoringLocationWqx)
				.build();
	}

	public Step transformMonitoringLocationStoretw() {
		return stepBuilderFactory.get("transformMonitoringLocationStoretw")
				.tasklet(transformMonitoringLocationStoretw)
				.build();
	}

	public Step buildMonitoringLocationIndexes() {
		return stepBuilderFactory.get("buildMonitoringLocationIndexes")
				.tasklet(buildMonitoringLocationIndexes)
				.build();
	}
}
