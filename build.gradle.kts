plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.azure:azure-messaging-servicebus:7.10.1")
    implementation("com.azure:azure-identity:1.5.4")
    implementation("com.azure.resourcemanager:azure-resourcemanager-servicebus:2.18.0")
}