package org.example;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.azure.resourcemanager.servicebus.ServiceBusManager;
import com.azure.resourcemanager.servicebus.models.ServiceBusNamespace;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static java.util.UUID.randomUUID;

public class Main {
    public static void main(String[] args) {
        var props = getProperties();
        String connectionString = props.getProperty("service.bus.connection.string");
        String topicName = props.getProperty("service.bus.topic.name");
        var subscriptionName = "temp_" + randomUUID().toString().substring(0, 16);

        var namespace = createSubscription(props, topicName, subscriptionName);

        var sender = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .topicName(topicName)
                .buildClient();

        sendMessages(sender);

        var receiver = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .topicName(topicName)
                .subscriptionName(subscriptionName)
                // To work around the problem,
                // Change this to PEEK_LOCK:
                .receiveMode(ServiceBusReceiveMode.RECEIVE_AND_DELETE)
                // Or this to more than 1:
                .prefetchCount(0)
                .buildClient();

        var discardPile = receiver.receiveMessages(5, Duration.ofSeconds(10)).stream().toList();

        sendMessages(sender);

        var messages = receiver.receiveMessages(5, Duration.ofSeconds(10));

        System.out.println("Received messages:");
        messages.forEach(m -> System.out.println(m.getBody().toString()));


        System.out.println("Discard messages:");
        discardPile.forEach(m -> System.out.println(m.getBody().toString()));

        namespace.topics().getByName(topicName).subscriptions().deleteByName(subscriptionName);

        System.exit(0);
    }

    private static void sendMessages(ServiceBusSenderClient sender) {
        Random rand = new Random();
        var messages = List.of("example1:" + rand.nextInt(1000),
                                            "example2:" + rand.nextInt(1000));
        messages.forEach(message ->  sender.sendMessage(new ServiceBusMessage(message)));

        System.out.println("Expected messages:");
        messages.forEach(m -> System.out.println(m));
    }

    private static ServiceBusNamespace createSubscription(Properties props, String topic, String subscriptionName) {
        String tenantId = props.getProperty("azure.tenantId");
        AzureProfile profile = new AzureProfile(tenantId, props.getProperty("azure.subscriptionId"), AzureEnvironment.AZURE);
        TokenCredential credential = new DefaultAzureCredentialBuilder()
                .authorityHost(profile.getEnvironment().getActiveDirectoryEndpoint())
                .tenantId(tenantId)
                .build();
        var serviceBusManager = ServiceBusManager
                .authenticate(credential, profile);

        var serviceBusResourceGroup = props.getProperty("service.bus.resourcegroup");
        var serviceBusNamespaceName = props.getProperty("service.bus.namespace");

        var serviceBusNamespace = serviceBusManager.namespaces()
                .getByResourceGroup(serviceBusResourceGroup, serviceBusNamespaceName);

        serviceBusNamespace.topics()
                .getByName(topic)
                .subscriptions()
                .define(subscriptionName)
                .withDeleteOnIdleDurationInMinutes(10)
                .create();

        return serviceBusNamespace;
    }
    public static Properties getProperties() {
        try {
            Properties props = new Properties();
            props.load(Main.class.getResourceAsStream("/application.properties"));
            return props;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}