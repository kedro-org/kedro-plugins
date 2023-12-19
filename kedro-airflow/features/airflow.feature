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
    And I should get a message including "create-model-input-table-node"
    And I should get a message including "preprocess-companies-node"
    And I should get a message including "preprocess-shuttles-node"

  Scenario: Run Airflow task locally with latest Kedro
    Given I have installed kedro version "latest"
    And I have prepared a config file
    And I have run a non-interactive kedro new
    And I have executed the kedro command "airflow create -t ../airflow/dags/"
    And I have installed the kedro project package
    When I execute the airflow command "tasks test project-dummy preprocess-companies-node"
    Then I should get a successful exit code
    And I should get a message including "Loading data from companies"
