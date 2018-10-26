package gov.acwi.nwqmc.etl.project;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class ProjectTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dropProjectIndexes")
	public Tasklet dropProjectIndexes;

	@Autowired
	@Qualifier("truncateProject")
	public Tasklet truncateProject;

	@Autowired
	@Qualifier("transformProjectWqx")
	public Tasklet transformProjectWqx;

	@Autowired
	@Qualifier("transformProjectStoretw")
	public Tasklet transformProjectStoretw;

	@Autowired
	@Qualifier("buildProjectIndexes")
	public Tasklet buildProjectIndexes;

	public SimpleJobBuilder build(SimpleJobBuilder job) {
		return job.next(dropProjectIndexes())
				.next(truncateProject())
				.next(transformProjectWqx())
				.next(transformProjectStoretw())
				.next(buildProjectIndexes());
	}

	public Step dropProjectIndexes() {
		return stepBuilderFactory.get("dropProjectIndexes")
				.tasklet(dropProjectIndexes)
				.build();
	}

	public Step truncateProject() {
		return stepBuilderFactory.get("truncateProject")
				.tasklet(truncateProject)
				.build();
	}

	public Step transformProjectWqx() {
		return stepBuilderFactory.get("transformProjectWqx")
				.tasklet(transformProjectWqx)
				.build();
	}

	public Step transformProjectStoretw() {
		return stepBuilderFactory.get("transformProjectStoretw")
				.tasklet(transformProjectStoretw)
				.build();
	}

	public Step buildProjectIndexes() {
		return stepBuilderFactory.get("buildProjectIndexes")
				.tasklet(buildProjectIndexes)
				.build();
	}
}
