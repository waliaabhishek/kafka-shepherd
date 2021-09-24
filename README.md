<img src="https://github.com/waliaabhishek/kafka-shepherd/workflows/Build%20&%20Unit%20Tests/badge.svg" class="image mod-full-width" /> <img src="https://img.shields.io/github/v/release/waliaabhishek/kafka-shepherd?sort=semver" class="image mod-full-width" /> <img src="https://goreportcard.com/badge/github.com/waliaabhishek/kafka-shepherd" class="image mod-full-width" /> 

# Kafka Shepherd

## Mission Statement 
* Make life of a Kafka Administrator as simple as possible and codify most aspects of managing Kafka object lifecycle. 
* Follow the GitOps model to manage the Kafka Objects in git repositories and simply allow the administrator to achieve the desired state by running a single command. 
* This includes managing:
  * Kafka Topic Lifecycle
  * Kafka ACL Lifecycle
  * Kafka Connector ACLs
  * Confluent ksqlDB ACLs
  * Migrating ACLs from one cluster to another

and so forth.... 

## Introduction
Managing a Kafka Cluster is hard. 
There are so many things to worry about and when the team tries to steer towards an automated object management goal; Kafka Object management scripts start showing up everywhere. 
Kafka Shepherd is a project that envisions to help you solve all your Kafka Object nmanagement problem with a single toolkit. 
All you need is a grasp over YAML and a good understanding of what your end state view of the cluster looks like. 
Once you execute Kafka Shepherd with the provided YAML's, it ensures that the end state goal is reached. 

The YAML configurations makes life of an operator pretty easy to manage everything in a Versioned repository, maintaining full audit trail about what was created when and why. 

