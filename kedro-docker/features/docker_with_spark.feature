@docker
Feature: Docker commands in new Spark projects
  Background:
    Given I have prepared a config file
    And I run a non-interactive kedro new using spaceflights-pyspark starter
    And I have installed the project dependencies
    And I have removed old docker image of test project

  Scenario: Execute docker init --with-spark
    When I execute the kedro command "docker init --with-spark"
    Then I should get a successful exit code
    And A Dockerfile file should exist
    And A Dockerfile file should contain default-jre-headless string
    And A .dive-ci file should exist
    And A .dockerignore file should exist

  Scenario: Execute docker build and run using spark Dockerfile
    When I execute the kedro command "docker build --with-spark"
    Then I should get a successful exit code
    And A new docker image for test project should be created
    When I execute the kedro command "docker run"
    Then I should get a successful exit code
    And I should get a message including "Pipeline execution completed"
