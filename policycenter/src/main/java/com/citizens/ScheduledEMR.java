package com.citizens;

import io.github.cdklabs.cdknag.AwsSolutionsChecks;
import io.github.cdklabs.cdknag.NagPackProps;
import software.amazon.awscdk.App;
import software.amazon.awscdk.Aspects;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;

import java.io.IOException;

/**
 An application class that instantiates respective CDK Apps for respective environments.
 This class will be used to run CDK nag for the respective CDK App to ensure compliance

 Author: Jay Mehta
 * */

public final class ScheduledEMR {
    public static void main(final String[] args) throws IOException {
        App app = new App();

        //Used to initialize configuration object for the respective environment.
        //By default, the class will be used to initialize configuration object for "dev".
        // It should be modified based on the environments configured in the respective AWS Account
        // in liaison with the properties file directory.
        Configuration conf = new Configuration(app.getNode().tryGetContext("environment").toString());

        //Set up Environment. This is required to lookup VPC in the account.
        Environment envWRG = Environment.builder()
                .account(conf.getCustomerAccountId())
                .region(conf.getCustomerRegion())
                .build();

        new PersistentPrerequisites(app, conf.getEntitiesParamValue()+"-prerequisites", StackProps.builder().env(envWRG).build());

        new IAMConfig(app, conf.getEntitiesParamValue()+"-iamconfig", StackProps.builder().env(envWRG).build());

        new UtilizedEMRStack(app, conf.getEntitiesParamValue()+"-emr", StackProps.builder().env(envWRG).build());

        //CDK Nag that will produce CSV output in cdk.out directory
        Aspects.of(app).add(new AwsSolutionsChecks(NagPackProps.builder().logIgnores(true).reports(true).verbose(true).build()));

        app.synth();
    }
}
