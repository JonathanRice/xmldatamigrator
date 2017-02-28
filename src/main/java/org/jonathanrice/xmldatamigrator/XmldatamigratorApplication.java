package org.jonathanrice.xmldatamigrator;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication
@EnableAutoConfiguration(exclude={DataSourceAutoConfiguration.class})
public class XmldatamigratorApplication implements CommandLineRunner {

	@Override
	public void run(String... args) {
		System.out.println("Hello World");
		if (args.length > 0 && args[0].equals("exitcode")) {
			//TODO validations throw new ExitException();
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(XmldatamigratorApplication.class, args);
	}
}
