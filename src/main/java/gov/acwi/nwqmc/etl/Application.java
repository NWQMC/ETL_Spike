package gov.acwi.nwqmc.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application { //implements CommandLineRunner {
	private static final Logger LOG = LoggerFactory.getLogger(Application.class);

	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	Job job;

	public static void main(String[] args) {
		LOG.info(args.toString());
		SpringApplication.run(Application.class, args);
	}

//	@Override
//	public void run(String... args) throws Exception {
//		JobParameters params = new JobParametersBuilder()
//				.addString("JobID", String.valueOf(System.currentTimeMillis()))
//				.addString("datasource", "STORET")
//				.toJobParameters();
//		jobLauncher.run(job, params);
//	}
}
