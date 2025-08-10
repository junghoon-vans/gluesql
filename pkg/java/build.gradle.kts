plugins {
    `java-library`
    `maven-publish`
    id("com.diffplug.spotless") version "6.25.0"
}

group = "org.gluesql"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

val jacksonVersion = "2.19.2"
val junitVersion = "5.10.2"

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

sourceSets {
    main {
        java {
            setSrcDirs(listOf("src/main/java"))
        }
        resources {
            setSrcDirs(listOf("src/main/resources"))
        }
    }
    test {
        java {
            setSrcDirs(listOf("src/test/java"))
        }
    }
}

tasks.test {
    useJUnitPlatform()
    
    // Set java.library.path to include our target/native/release directory
    systemProperty("java.library.path", "${project.projectDir}/target/native/release")
}

// Task to build the native Rust library
val buildRustLibrary by tasks.registering(Exec::class) {
    description = "Build the Rust native library using Cargo"
    group = "build"
    
    workingDir(project.projectDir)
    commandLine("cargo", "build", "--release", "--target-dir", "target/native")
    
    inputs.files(fileTree("src").include("**/*.rs"))
    inputs.file("Cargo.toml")
    
    outputs.dir("target/native/release")
}

// Make the Java compilation and test depend on the native library
tasks.compileJava {
    dependsOn(buildRustLibrary)
}

tasks.processResources {
    dependsOn(buildRustLibrary)
}

tasks.test {
    dependsOn(buildRustLibrary)
}

tasks.jar {
    archiveBaseName.set("gluesql-java")
    
    manifest {
        attributes(
            "Implementation-Title" to "GlueSQL Java Bindings",
            "Implementation-Version" to project.version,
            "Implementation-Vendor" to "GlueSQL"
        )
    }
}

spotless {
    java {
       palantirJavaFormat()
       removeUnusedImports()
       trimTrailingWhitespace()
       endWithNewline()
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            
            pom {
                name.set("GlueSQL Java Bindings")
                description.set("Java bindings for GlueSQL - an open-source SQL database engine")
                url.set("https://github.com/gluesql/gluesql")
                
                licenses {
                    license {
                        name.set("Apache License 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                
                developers {
                    developer {
                        id.set("gluesql")
                        name.set("GlueSQL Team")
                        email.set("contact@gluesql.org")
                    }
                }
                
                scm {
                    connection.set("scm:git:git://github.com/gluesql/gluesql.git")
                    developerConnection.set("scm:git:ssh://github.com:gluesql/gluesql.git")
                    url.set("https://github.com/gluesql/gluesql")
                }
            }
        }
    }
}
