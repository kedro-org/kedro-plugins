Feature: Airflow

  Background:
    Given I have initialized Airflow with home dir "airflow"

  Scenario: Print a list of tasks with latest Kedro
    Given I have installed kedro version "latest"
    And I have prepared a config file
    And I have run a non-interactive kedro new
    And I have executed the kedro command "airflow create -t ../airflow/dags/"
    When I execute the airflow command "tasks list project-dummy"
    Then I should get a successful exit code
    And I should get a message including "split"
    And I should get a message including "train"
    And I should get a message including "predict"

  Scenario: Run Airflow task locally with latest Kedro
    Given I have installed kedro version "latest"
    And I have prepared a config file
    And I have run a non-interactive kedro new
    And I have executed the kedro command "airflow create -t ../airflow/dags/"
    And I have installed the kedro project package
    When I execute the airflow command "tasks test project-dummy split"
    Then I should get a successful exit code
    And I should get a message including "Loading data"
