import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.io.*;
import java.lang.*;
import java.util.LinkedList;
import java.util.List;

public class EMR {

    public static final String ROOT_PATH = "s3://dsp-ass-3/data/";

    public static final String DATASET_PATH = "s3://dsp-ass-3/data/word-relatedness.txt";
    public static final String CORPUS_PATH = "";
    public static final String FEATURES_PATH = "s3://dsp-ass-3/data/FeaturesOcc/";


    public static void main(String[] args) throws Exception {

//M4_XLARGE
        EmrClient mapReduce = EmrClient.builder().credentialsProvider(ProfileCredentialsProvider.builder().profileName("default").build()).build();


        List<StepConfig> steps = new LinkedList<>();
        steps.add(createStepConfig("s3n://assignment3dsp/biarcs/biarcs.00-of-99", "out-semi", "CountLexemeAndFeatures"));
        steps.add(createStepConfig("out-semi", "FeaturesOcc", "WriteTotalFeaturesOcc"));
        steps.add(createStepConfig("out-semi", "finalOutput-semi", "CalcVectors"));

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(4)
                .masterInstanceType(InstanceType.M4_XLARGE.toString())
                .slaveInstanceType(InstanceType.M4_XLARGE.toString())
                .hadoopVersion("2.7.3")
                .keepJobFlowAliveWhenNoSteps(false)
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build()).build();

        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name("ass3")
                .releaseLabel("emr-5.0.3")
                .instances(instances)
                .steps(steps)
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .logUri("s3://dsp-ass-3/logs/").build();


        RunJobFlowResponse runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.jobFlowId();

        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static StepConfig createStepConfig(String input, String output, String mainClass) {
        String inputArg = ROOT_PATH + input + "/";
        String outputArg = ROOT_PATH + output + "/";
        if (mainClass.equals("CountLexemeAndFeatures")) {
            inputArg = input;
        }
        HadoopJarStepConfig hadoopJarStep = HadoopJarStepConfig.builder()
                .jar("s3://dsp-ass-3/jars/" + mainClass + ".jar") // This should be a full map reduce application.
                .mainClass(mainClass)
                .args(inputArg, outputArg).build();

        StepConfig stepConfig = StepConfig.builder()
                .name("step - " + mainClass)
                .hadoopJarStep(hadoopJarStep)
                .actionOnFailure("TERMINATE_JOB_FLOW").build();
        return stepConfig;

    }


}
